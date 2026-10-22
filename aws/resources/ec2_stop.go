package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/gruntwork-io/cloud-nuke/config"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/gruntwork-io/go-commons/errors"
)

// ec2StateWaitTimeout bounds waits for an instance to settle into running or stopped.
const ec2StateWaitTimeout = 10 * time.Minute

// ec2EKSNodegroupTag marks instances owned by an EKS managed node group. Those are scaled
// to zero by the eks-cluster stop hooks; stopping them here would make their Auto Scaling
// group replace them.
const ec2EKSNodegroupTag = "eks:nodegroup-name"

// ec2PrevState is what Start needs to restore an instance.
type ec2PrevState struct {
	State string `json:"state"`
}

func ec2InstanceStopHooks() *resource.StopHooks[EC2InstancesAPI] {
	return &resource.StopHooks[EC2InstancesAPI]{
		List:    listRunningEC2Instances,
		Capture: captureEC2Instance,
		Stop:    stopEC2Instance,
		Start:   startEC2Instance,
	}
}

// listRunningEC2Instances lists running and pending instances. Unlike the nuke lister it keeps
// termination-protected instances: protection does not prevent stopping.
func listRunningEC2Instances(ctx context.Context, client EC2InstancesAPI, _ resource.Scope, cfg config.ResourceType) ([]*string, error) {
	paginator := ec2.NewDescribeInstancesPaginator(client, &ec2.DescribeInstancesInput{
		Filters: []types.Filter{{
			Name:   aws.String("instance-state-name"),
			Values: []string{"running", "pending"},
		}},
	})
	var ids []*string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, errors.WithStackTrace(err)
		}
		for _, res := range page.Reservations {
			for _, inst := range res.Instances {
				if hasEC2Tag(inst.Tags, ec2EKSNodegroupTag) {
					continue
				}
				if shouldIncludeInstanceId(inst, false, cfg) {
					ids = append(ids, inst.InstanceId)
				}
			}
		}
	}
	return ids, nil
}

func hasEC2Tag(tags []types.Tag, key string) bool {
	for _, t := range tags {
		if aws.ToString(t.Key) == key {
			return true
		}
	}
	return false
}

// describeEC2Instance returns the instance, or nil if it does not exist.
func describeEC2Instance(ctx context.Context, client EC2InstancesAPI, id *string) (*types.Instance, error) {
	out, err := client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{aws.ToString(id)}})
	if err != nil {
		if hasErrorCode(err, "InvalidInstanceID.NotFound", "InvalidInstanceID.Malformed") {
			return nil, nil
		}
		return nil, errors.WithStackTrace(err)
	}
	for _, res := range out.Reservations {
		for _, inst := range res.Instances {
			if aws.ToString(inst.InstanceId) == aws.ToString(id) {
				return &inst, nil
			}
		}
	}
	return nil, nil
}

func ec2StateName(inst *types.Instance) types.InstanceStateName {
	if inst == nil || inst.State == nil {
		return ""
	}
	return inst.State.Name
}

func captureEC2Instance(ctx context.Context, client EC2InstancesAPI, id *string) (json.RawMessage, error) {
	inst, err := describeEC2Instance(ctx, client, id)
	if err != nil {
		return nil, err
	}
	switch ec2StateName(inst) {
	case types.InstanceStateNameRunning:
	case types.InstanceStateNamePending:
		// Wait so the stop call is not rejected for a transitional state.
		if err := ec2.NewInstanceRunningWaiter(client).Wait(ctx, &ec2.DescribeInstancesInput{
			InstanceIds: []string{aws.ToString(id)},
		}, ec2StateWaitTimeout); err != nil {
			return nil, fmt.Errorf("waiting for pending instance to run: %w", err)
		}
	default:
		// stopping, stopped, shutting-down, terminated or gone.
		return nil, resource.ErrAlreadyStopped
	}
	if inst.RootDeviceType == types.DeviceTypeInstanceStore {
		return nil, resource.Unsupported("instance-store root volume")
	}
	return marshalPrev(ec2PrevState{State: string(types.InstanceStateNameRunning)})
}

func stopEC2Instance(ctx context.Context, client EC2InstancesAPI, id *string, _ json.RawMessage) error {
	_, err := client.StopInstances(ctx, &ec2.StopInstancesInput{InstanceIds: []string{aws.ToString(id)}})
	if err != nil {
		// e.g. a Spot Instance from a one-time request.
		if hasErrorCode(err, "UnsupportedOperation", "UnsupportedInstanceAttribute") {
			return resource.Unsupported("%v", err)
		}
		return errors.WithStackTrace(err)
	}
	return nil
}

// startEC2Instance starts the instance and waits until it is running, so that Auto Scaling
// groups (resumed after EC2) see healthy instances.
func startEC2Instance(ctx context.Context, client EC2InstancesAPI, id *string, prev json.RawMessage) error {
	var p ec2PrevState
	if err := unmarshalPrev(prev, &p); err != nil {
		return err
	}
	if p.State != string(types.InstanceStateNameRunning) {
		return nil
	}
	input := &ec2.DescribeInstancesInput{InstanceIds: []string{aws.ToString(id)}}

	inst, err := describeEC2Instance(ctx, client, id)
	if err != nil {
		return err
	}
	switch ec2StateName(inst) {
	case "", types.InstanceStateNameShuttingDown, types.InstanceStateNameTerminated:
		return resource.ErrGone
	case types.InstanceStateNameRunning:
		return nil
	case types.InstanceStateNamePending:
		return ec2.NewInstanceRunningWaiter(client).Wait(ctx, input, ec2StateWaitTimeout)
	case types.InstanceStateNameStopping:
		if err := ec2.NewInstanceStoppedWaiter(client).Wait(ctx, input, ec2StateWaitTimeout); err != nil {
			return fmt.Errorf("waiting for instance to finish stopping: %w", err)
		}
	}

	if _, err := client.StartInstances(ctx, &ec2.StartInstancesInput{InstanceIds: []string{aws.ToString(id)}}); err != nil {
		if hasErrorCode(err, "InvalidInstanceID.NotFound") {
			return resource.ErrGone
		}
		return errors.WithStackTrace(err)
	}
	if err := ec2.NewInstanceRunningWaiter(client).Wait(ctx, input, ec2StateWaitTimeout); err != nil {
		return fmt.Errorf("waiting for instance to run: %w", err)
	}
	return nil
}
