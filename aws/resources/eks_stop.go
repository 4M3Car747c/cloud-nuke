package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/gruntwork-io/cloud-nuke/logging"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/gruntwork-io/go-commons/errors"
)

// Stopping an EKS cluster means scaling all of its managed node groups to zero. The control
// plane cannot be stopped and keeps billing ($0.10/h); that is accepted. Not handled: pods on
// Fargate profiles keep running, self-managed node groups (plain ASGs, which the asg hooks
// only suspend) and Karpenter-provisioned nodes keep running, and add-ons are left as they are.
// Scaling a node group to zero drains its nodes; the last node can take ~15 minutes to go
// away when a PodDisruptionBudget (e.g. coredns) blocks eviction.

// eksNodegroupWaitTimeout bounds waits for a node group to leave CREATING / UPDATING.
const eksNodegroupWaitTimeout = 20 * time.Minute

// eksUpdateRetries bounds how often an UpdateNodegroupConfig rejected with
// ResourceInUseException (another update in progress) is retried after waiting.
const eksUpdateRetries = 5

// eksNodegroupPollDelay is the delay between DescribeNodegroup polls while waiting. Tests
// shorten it.
var eksNodegroupPollDelay = 15 * time.Second

// eksNodegroupScaling is the scaling config of one managed node group.
type eksNodegroupScaling struct {
	Min     int32 `json:"min"`
	Max     int32 `json:"max"`
	Desired int32 `json:"desired"`
}

// eksPrevState is what Start needs to restore a cluster: the scaling config of every managed
// node group that had (or could have) nodes.
type eksPrevState struct {
	Nodegroups map[string]eksNodegroupScaling `json:"nodegroups"`
}

func eksClusterStopHooks() *resource.StopHooks[EKSClustersAPI] {
	return &resource.StopHooks[EKSClustersAPI]{
		// The nuke lister (listEKSClusters) passes cluster tags to cfg.ShouldInclude, so it is used as is.
		Capture: captureEKSCluster,
		Stop:    stopEKSCluster,
		Start:   startEKSCluster,
	}
}

// eksNodegroupTransitional reports whether a node group is in a state that has to be waited out
// before its scaling config can be read or changed.
func eksNodegroupTransitional(s ekstypes.NodegroupStatus) bool {
	return s == ekstypes.NodegroupStatusCreating || s == ekstypes.NodegroupStatusUpdating
}

// describeEKSNodegroup returns the node group once it is no longer CREATING or UPDATING, or nil
// if it does not exist (or is being deleted).
func describeEKSNodegroup(ctx context.Context, client EKSClustersAPI, cluster, name string) (*ekstypes.Nodegroup, error) {
	input := &eks.DescribeNodegroupInput{ClusterName: aws.String(cluster), NodegroupName: aws.String(name)}
	out, err := client.DescribeNodegroup(ctx, input)
	if err != nil {
		if hasErrorCode(err, "ResourceNotFoundException") {
			return nil, nil
		}
		return nil, errors.WithStackTrace(err)
	}
	ng := out.Nodegroup
	if ng != nil && eksNodegroupTransitional(ng.Status) {
		logging.Debugf("Waiting for EKS node group %s/%s (%s)", cluster, name, ng.Status)
		waiter := eks.NewNodegroupActiveWaiter(client, func(o *eks.NodegroupActiveWaiterOptions) {
			o.MinDelay = eksNodegroupPollDelay
			o.MaxDelay = eksNodegroupPollDelay
			o.Retryable = func(_ context.Context, _ *eks.DescribeNodegroupInput, out *eks.DescribeNodegroupOutput, err error) (bool, error) {
				if err != nil {
					if hasErrorCode(err, "ResourceNotFoundException") {
						return false, nil
					}
					return false, err
				}
				return out.Nodegroup != nil && eksNodegroupTransitional(out.Nodegroup.Status), nil
			}
		})
		if err := waiter.Wait(ctx, input, eksNodegroupWaitTimeout); err != nil {
			return nil, fmt.Errorf("waiting for EKS node group %s/%s to settle: %w", cluster, name, err)
		}
		out, err = client.DescribeNodegroup(ctx, input)
		if err != nil {
			if hasErrorCode(err, "ResourceNotFoundException") {
				return nil, nil
			}
			return nil, errors.WithStackTrace(err)
		}
		ng = out.Nodegroup
	}
	if ng == nil || ng.Status == ekstypes.NodegroupStatusDeleting {
		return nil, nil
	}
	return ng, nil
}

func eksScaling(ng *ekstypes.Nodegroup) eksNodegroupScaling {
	var s eksNodegroupScaling
	if c := ng.ScalingConfig; c != nil {
		s.Min = aws.ToInt32(c.MinSize)
		s.Max = aws.ToInt32(c.MaxSize)
		s.Desired = aws.ToInt32(c.DesiredSize)
	}
	return s
}

// eksClusterExists reports whether the cluster exists.
func eksClusterExists(ctx context.Context, client EKSClustersAPI, cluster string) (bool, error) {
	_, err := client.DescribeCluster(ctx, &eks.DescribeClusterInput{Name: aws.String(cluster)})
	if err != nil {
		if hasErrorCode(err, "ResourceNotFoundException") {
			return false, nil
		}
		return false, errors.WithStackTrace(err)
	}
	return true, nil
}

func listEKSNodegroupNames(ctx context.Context, client EKSClustersAPI, cluster string) ([]string, error) {
	var names []string
	paginator := eks.NewListNodegroupsPaginator(client, &eks.ListNodegroupsInput{ClusterName: aws.String(cluster)})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, errors.WithStackTrace(err)
		}
		names = append(names, page.Nodegroups...)
	}
	sort.Strings(names)
	return names, nil
}

func eksHasFargateProfiles(ctx context.Context, client EKSClustersAPI, cluster string) (bool, error) {
	out, err := client.ListFargateProfiles(ctx, &eks.ListFargateProfilesInput{ClusterName: aws.String(cluster)})
	if err != nil {
		return false, errors.WithStackTrace(err)
	}
	return len(out.FargateProfileNames) > 0, nil
}

func captureEKSCluster(ctx context.Context, client EKSClustersAPI, id *string) (json.RawMessage, error) {
	cluster := aws.ToString(id)
	exists, err := eksClusterExists(ctx, client, cluster)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, resource.ErrAlreadyStopped
	}
	names, err := listEKSNodegroupNames(ctx, client, cluster)
	if err != nil {
		return nil, err
	}
	prev := eksPrevState{Nodegroups: map[string]eksNodegroupScaling{}}
	found := 0
	for _, name := range names {
		ng, err := describeEKSNodegroup(ctx, client, cluster, name)
		if err != nil {
			return nil, err
		}
		if ng == nil {
			continue
		}
		found++
		if s := eksScaling(ng); s.Desired > 0 || s.Min > 0 {
			prev.Nodegroups[name] = s
		}
	}
	if found == 0 {
		// Nothing we can scale down. If pods run on Fargate the cluster still bills for them.
		fargate, err := eksHasFargateProfiles(ctx, client, cluster)
		if err != nil {
			return nil, err
		}
		if fargate {
			return nil, resource.Unsupported("EKS cluster has Fargate profiles and no managed node groups")
		}
	}
	if len(prev.Nodegroups) == 0 {
		return nil, resource.ErrAlreadyStopped
	}
	return marshalPrev(prev)
}

// updateEKSNodegroupScaling sets the scaling config of a node group, waiting out and retrying
// updates already in progress. It returns false if the node group no longer exists.
func updateEKSNodegroupScaling(ctx context.Context, client EKSClustersAPI, cluster, name string, want func(cur eksNodegroupScaling) eksNodegroupScaling) (bool, error) {
	for attempt := 0; ; attempt++ {
		ng, err := describeEKSNodegroup(ctx, client, cluster, name)
		if err != nil {
			return false, err
		}
		if ng == nil {
			return false, nil
		}
		cur := eksScaling(ng)
		target := want(cur)
		if cur == target {
			return true, nil
		}
		_, err = client.UpdateNodegroupConfig(ctx, &eks.UpdateNodegroupConfigInput{
			ClusterName:   aws.String(cluster),
			NodegroupName: aws.String(name),
			ScalingConfig: &ekstypes.NodegroupScalingConfig{
				MinSize:     aws.Int32(target.Min),
				MaxSize:     aws.Int32(target.Max),
				DesiredSize: aws.Int32(target.Desired),
			},
		})
		switch {
		case err == nil:
			return true, nil
		case hasErrorCode(err, "ResourceNotFoundException"):
			return false, nil
		case hasErrorCode(err, "ResourceInUseException") && attempt < eksUpdateRetries:
			// Another update is in progress; describeEKSNodegroup waits for it on the next pass.
			logging.Debugf("EKS node group %s/%s busy, retrying: %v", cluster, name, err)
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-time.After(eksNodegroupPollDelay):
			}
		default:
			return false, errors.WithStackTrace(err)
		}
	}
}

func stopEKSCluster(ctx context.Context, client EKSClustersAPI, id *string, prev json.RawMessage) error {
	var p eksPrevState
	if err := unmarshalPrev(prev, &p); err != nil {
		return err
	}
	cluster := aws.ToString(id)
	for _, name := range sortedEKSNodegroups(p) {
		_, err := updateEKSNodegroupScaling(ctx, client, cluster, name, func(cur eksNodegroupScaling) eksNodegroupScaling {
			if cur.Min == 0 && cur.Desired == 0 {
				return cur
			}
			// MaxSize must stay >= 1, so keep it.
			return eksNodegroupScaling{Min: 0, Max: cur.Max, Desired: 0}
		})
		if err != nil {
			return fmt.Errorf("scaling EKS node group %s/%s to zero: %w", cluster, name, err)
		}
	}
	return nil
}

func startEKSCluster(ctx context.Context, client EKSClustersAPI, id *string, prev json.RawMessage) error {
	var p eksPrevState
	if err := unmarshalPrev(prev, &p); err != nil {
		return err
	}
	cluster := aws.ToString(id)
	exists, err := eksClusterExists(ctx, client, cluster)
	if err != nil {
		return err
	}
	if !exists {
		return resource.ErrGone
	}
	restored := 0
	for _, name := range sortedEKSNodegroups(p) {
		target := p.Nodegroups[name]
		if target.Max < 1 || target.Min < 0 || target.Min > target.Desired || target.Desired > target.Max {
			return fmt.Errorf("EKS node group %s/%s: invalid saved scaling config %+v", cluster, name, target)
		}
		ok, err := updateEKSNodegroupScaling(ctx, client, cluster, name, func(eksNodegroupScaling) eksNodegroupScaling {
			return target
		})
		if err != nil {
			return fmt.Errorf("restoring EKS node group %s/%s: %w", cluster, name, err)
		}
		if ok {
			restored++
		} else {
			logging.Warnf("EKS node group %s/%s no longer exists; not restored", cluster, name)
		}
	}
	if restored == 0 && len(p.Nodegroups) > 0 {
		return resource.ErrGone
	}
	return nil
}

func sortedEKSNodegroups(p eksPrevState) []string {
	names := make([]string, 0, len(p.Nodegroups))
	for name := range p.Nodegroups {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
