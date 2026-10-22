package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/applicationautoscaling"
	aastypes "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling/types"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/gruntwork-io/go-commons/errors"
)

// ecsServicePrevState is what Start needs to restore a service. Start runs in a later
// process that has not listed anything, so the cluster has to be saved too.
type ecsServicePrevState struct {
	Cluster      string `json:"cluster"`
	DesiredCount int32  `json:"desired_count"`
	// Scaling is the service's Application Auto Scaling suspension state before the stop,
	// or nil when the service has no scalable target.
	Scaling *ecsScalingSuspended `json:"scaling,omitempty"`
}

// ecsScalingSuspended mirrors aastypes.SuspendedState with plain bools.
type ecsScalingSuspended struct {
	DynamicIn  bool `json:"dynamic_in"`
	DynamicOut bool `json:"dynamic_out"`
	Scheduled  bool `json:"scheduled"`
}

const ecsScalableDimension = aastypes.ScalableDimensionECSServiceDesiredCount

// ecsScalingResourceID is the Application Auto Scaling resource ID of a service:
// service/<cluster name>/<service name>.
func ecsScalingResourceID(clusterArn, serviceArn string) string {
	cluster := clusterArn[strings.LastIndex(clusterArn, "/")+1:]
	service := serviceArn[strings.LastIndex(serviceArn, "/")+1:]
	return "service/" + cluster + "/" + service
}

// ecsScalableTarget returns the service's scalable target, or nil if it has none.
func ecsScalableTarget(ctx context.Context, client ECSServicesAPI, resourceID string) (*aastypes.ScalableTarget, error) {
	out, err := client.DescribeScalableTargets(ctx, &applicationautoscaling.DescribeScalableTargetsInput{
		ServiceNamespace:  aastypes.ServiceNamespaceEcs,
		ResourceIds:       []string{resourceID},
		ScalableDimension: ecsScalableDimension,
	})
	if err != nil {
		return nil, errors.WithStackTrace(err)
	}
	for i := range out.ScalableTargets {
		if aws.ToString(out.ScalableTargets[i].ResourceId) == resourceID {
			return &out.ScalableTargets[i], nil
		}
	}
	return nil, nil
}

// setECSScalingSuspended updates only the suspension state of an existing scalable target.
func setECSScalingSuspended(ctx context.Context, client ECSServicesAPI, resourceID string, s ecsScalingSuspended) error {
	_, err := client.RegisterScalableTarget(ctx, &applicationautoscaling.RegisterScalableTargetInput{
		ServiceNamespace:  aastypes.ServiceNamespaceEcs,
		ResourceId:        aws.String(resourceID),
		ScalableDimension: ecsScalableDimension,
		SuspendedState: &aastypes.SuspendedState{
			DynamicScalingInSuspended:  aws.Bool(s.DynamicIn),
			DynamicScalingOutSuspended: aws.Bool(s.DynamicOut),
			ScheduledScalingSuspended:  aws.Bool(s.Scheduled),
		},
	})
	return errors.WithStackTrace(err)
}

// ecsServiceStopHooks scales REPLICA services to zero tasks. IDs are service ARNs, as the
// nuke lister returns them; clusterOf finds a service's cluster from the lister's map.
func ecsServiceStopHooks(clusterOf func(serviceArn string) string) *resource.StopHooks[ECSServicesAPI] {
	return &resource.StopHooks[ECSServicesAPI]{
		Capture: func(ctx context.Context, client ECSServicesAPI, id *string) (json.RawMessage, error) {
			return captureECSService(ctx, client, id, clusterOf)
		},
		Stop:  stopECSService,
		Start: startECSService,
	}
}

// ecsClusterFromServiceArn returns the cluster ARN encoded in a new-format service ARN
// (arn:aws:ecs:region:account:service/cluster/name), or "" for the old format
// (arn:aws:ecs:region:account:service/name).
func ecsClusterFromServiceArn(serviceArn string) string {
	parts := strings.SplitN(serviceArn, ":", 6)
	if len(parts) != 6 {
		return ""
	}
	res := strings.Split(parts[5], "/")
	if len(res) != 3 || res[0] != "service" || res[1] == "" {
		return ""
	}
	return fmt.Sprintf("%s:%s:%s:%s:%s:cluster/%s", parts[0], parts[1], parts[2], parts[3], parts[4], res[1])
}

// describeECSService returns the service, or nil if it or its cluster does not exist.
func describeECSService(ctx context.Context, client ECSServicesAPI, cluster string, serviceArn *string) (*types.Service, error) {
	out, err := client.DescribeServices(ctx, &ecs.DescribeServicesInput{
		Cluster:  aws.String(cluster),
		Services: []string{aws.ToString(serviceArn)},
	})
	if err != nil {
		if hasErrorCode(err, "ClusterNotFoundException", "ServiceNotFoundException") {
			return nil, nil
		}
		return nil, errors.WithStackTrace(err)
	}
	for i := range out.Services {
		svc := out.Services[i]
		if aws.ToString(svc.ServiceArn) == aws.ToString(serviceArn) || aws.ToString(svc.ServiceName) == aws.ToString(serviceArn) {
			return &svc, nil
		}
	}
	// Missing services come back in Failures with reason MISSING.
	return nil, nil
}

// ecsServiceLive reports whether svc exists and is not being deleted.
func ecsServiceLive(svc *types.Service) bool {
	return svc != nil && aws.ToString(svc.Status) == "ACTIVE"
}

func captureECSService(ctx context.Context, client ECSServicesAPI, id *string, clusterOf func(string) string) (json.RawMessage, error) {
	arn := aws.ToString(id)
	cluster := ""
	if clusterOf != nil {
		cluster = clusterOf(arn)
	}
	if cluster == "" {
		cluster = ecsClusterFromServiceArn(arn)
	}
	if cluster == "" {
		return nil, fmt.Errorf("cannot tell the cluster of ECS service %s", arn)
	}
	svc, err := describeECSService(ctx, client, cluster, id)
	if err != nil {
		return nil, err
	}
	if !ecsServiceLive(svc) {
		return nil, resource.ErrAlreadyStopped
	}
	// DAEMON services run one task per container instance and cannot be scaled; their EC2
	// instances are stopped by the ec2/asg hooks, which stops the tasks too.
	if svc.SchedulingStrategy == types.SchedulingStrategyDaemon {
		return nil, resource.ErrAlreadyStopped
	}
	if svc.DesiredCount == 0 {
		return nil, resource.ErrAlreadyStopped
	}
	if c := aws.ToString(svc.ClusterArn); c != "" {
		cluster = c
	}
	prev := ecsServicePrevState{Cluster: cluster, DesiredCount: svc.DesiredCount}
	target, err := ecsScalableTarget(ctx, client, ecsScalingResourceID(cluster, arn))
	if err != nil {
		return nil, err
	}
	if target != nil {
		s := ecsScalingSuspended{}
		if st := target.SuspendedState; st != nil {
			s = ecsScalingSuspended{
				DynamicIn:  aws.ToBool(st.DynamicScalingInSuspended),
				DynamicOut: aws.ToBool(st.DynamicScalingOutSuspended),
				Scheduled:  aws.ToBool(st.ScheduledScalingSuspended),
			}
		}
		prev.Scaling = &s
	}
	return marshalPrev(prev)
}

// stopECSService suspends the service's auto scaling (so scaling policies, scheduled actions
// or a minimum capacity cannot bring tasks back while it is stopped), then scales it to zero.
func stopECSService(ctx context.Context, client ECSServicesAPI, id *string, prev json.RawMessage) error {
	var p ecsServicePrevState
	if err := unmarshalPrev(prev, &p); err != nil {
		return err
	}
	// Look the target up again rather than trusting prev, so one added since Capture is
	// suspended too.
	resourceID := ecsScalingResourceID(p.Cluster, aws.ToString(id))
	target, err := ecsScalableTarget(ctx, client, resourceID)
	if err != nil {
		return err
	}
	if target != nil {
		all := ecsScalingSuspended{DynamicIn: true, DynamicOut: true, Scheduled: true}
		if err := setECSScalingSuspended(ctx, client, resourceID, all); err != nil {
			return fmt.Errorf("suspending auto scaling: %w", err)
		}
	}
	_, err = client.UpdateService(ctx, &ecs.UpdateServiceInput{
		Cluster:      aws.String(p.Cluster),
		Service:      id,
		DesiredCount: aws.Int32(0),
	})
	if err != nil {
		// Deleted (or being deleted) meanwhile: nothing left to stop.
		if hasErrorCode(err, "ServiceNotFoundException", "ServiceNotActiveException", "ClusterNotFoundException") {
			return nil
		}
		return errors.WithStackTrace(err)
	}
	return nil
}

// startECSService sets desiredCount back, then restores the auto scaling suspension state.
// It does not wait for the tasks to become healthy.
func startECSService(ctx context.Context, client ECSServicesAPI, id *string, prev json.RawMessage) error {
	var p ecsServicePrevState
	if err := unmarshalPrev(prev, &p); err != nil {
		return err
	}
	if p.Cluster == "" {
		return fmt.Errorf("saved state for ECS service %s has no cluster", aws.ToString(id))
	}
	svc, err := describeECSService(ctx, client, p.Cluster, id)
	if err != nil {
		return err
	}
	if !ecsServiceLive(svc) {
		return resource.ErrGone
	}
	if svc.DesiredCount != p.DesiredCount {
		_, err = client.UpdateService(ctx, &ecs.UpdateServiceInput{
			Cluster:      aws.String(p.Cluster),
			Service:      id,
			DesiredCount: aws.Int32(p.DesiredCount),
		})
		if err != nil {
			if hasErrorCode(err, "ServiceNotFoundException", "ServiceNotActiveException", "ClusterNotFoundException") {
				return resource.ErrGone
			}
			return errors.WithStackTrace(err)
		}
	}
	if p.Scaling == nil {
		return nil
	}
	resourceID := ecsScalingResourceID(p.Cluster, aws.ToString(id))
	target, err := ecsScalableTarget(ctx, client, resourceID)
	if err != nil {
		return err
	}
	if target == nil {
		// The customer removed auto scaling while the service was stopped.
		return nil
	}
	if err := setECSScalingSuspended(ctx, client, resourceID, *p.Scaling); err != nil {
		return fmt.Errorf("restoring auto scaling: %w", err)
	}
	return nil
}
