package resources

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/gruntwork-io/cloud-nuke/config"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/gruntwork-io/cloud-nuke/util"
	"github.com/gruntwork-io/go-commons/errors"
)

// asgFreezeProcesses are suspended so a group cannot replace or re-launch instances while
// they are stopped.
var asgFreezeProcesses = []string{
	"Launch",
	"Terminate",
	"HealthCheck",
	"ReplaceUnhealthy",
	"AZRebalance",
	"AlarmNotification",
	"ScheduledActions",
}

// asgPrevState records which of asgFreezeProcesses were already suspended, so Start only
// resumes the ones the stop suspended.
type asgPrevState struct {
	Suspended []string `json:"suspended"`
}

func asgStopHooks() *resource.StopHooks[ASGroupsAPI] {
	return &resource.StopHooks[ASGroupsAPI]{
		List:    listStoppableASGroups,
		Capture: captureASG,
		Stop:    stopASG,
		Start:   startASG,
	}
}

// listStoppableASGroups lists groups except those owned by EKS managed node groups, which
// the eks-cluster stop hooks scale to zero instead.
func listStoppableASGroups(ctx context.Context, client ASGroupsAPI, _ resource.Scope, cfg config.ResourceType) ([]*string, error) {
	var names []*string
	paginator := autoscaling.NewDescribeAutoScalingGroupsPaginator(client, &autoscaling.DescribeAutoScalingGroupsInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, errors.WithStackTrace(err)
		}
		for _, g := range page.AutoScalingGroups {
			tags := util.ConvertAutoScalingTagsToMap(g.Tags)
			if _, ok := tags[ec2EKSNodegroupTag]; ok {
				continue
			}
			if cfg.ShouldInclude(config.ResourceValue{
				Time: g.CreatedTime,
				Name: g.AutoScalingGroupName,
				Tags: tags,
			}) {
				names = append(names, g.AutoScalingGroupName)
			}
		}
	}
	return names, nil
}

// describeASG returns the group, or nil if it does not exist.
func describeASG(ctx context.Context, client ASGroupsAPI, name *string) (*types.AutoScalingGroup, error) {
	out, err := client.DescribeAutoScalingGroups(ctx, &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []string{aws.ToString(name)},
	})
	if err != nil {
		return nil, errors.WithStackTrace(err)
	}
	for _, g := range out.AutoScalingGroups {
		if aws.ToString(g.AutoScalingGroupName) == aws.ToString(name) {
			return &g, nil
		}
	}
	return nil, nil
}

// suspendedFreezeProcesses returns which of asgFreezeProcesses are suspended on g.
func suspendedFreezeProcesses(g *types.AutoScalingGroup) []string {
	var out []string
	for _, p := range g.SuspendedProcesses {
		name := aws.ToString(p.ProcessName)
		if slices.Contains(asgFreezeProcesses, name) && !slices.Contains(out, name) {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

func captureASG(ctx context.Context, client ASGroupsAPI, name *string) (json.RawMessage, error) {
	g, err := describeASG(ctx, client, name)
	if err != nil {
		return nil, err
	}
	if g == nil {
		return nil, resource.ErrAlreadyStopped
	}
	suspended := suspendedFreezeProcesses(g)
	if len(suspended) == len(asgFreezeProcesses) {
		return nil, resource.ErrAlreadyStopped
	}
	if suspended == nil {
		suspended = []string{}
	}
	return marshalPrev(asgPrevState{Suspended: suspended})
}

func stopASG(ctx context.Context, client ASGroupsAPI, name *string, _ json.RawMessage) error {
	_, err := client.SuspendProcesses(ctx, &autoscaling.SuspendProcessesInput{
		AutoScalingGroupName: name,
		ScalingProcesses:     asgFreezeProcesses,
	})
	return errors.WithStackTrace(err)
}

func startASG(ctx context.Context, client ASGroupsAPI, name *string, prev json.RawMessage) error {
	var p asgPrevState
	if err := unmarshalPrev(prev, &p); err != nil {
		return err
	}
	g, err := describeASG(ctx, client, name)
	if err != nil {
		return err
	}
	if g == nil {
		return resource.ErrGone
	}
	var resume []string
	for _, proc := range asgFreezeProcesses {
		if !slices.Contains(p.Suspended, proc) {
			resume = append(resume, proc)
		}
	}
	if len(resume) == 0 {
		return nil
	}
	_, err = client.ResumeProcesses(ctx, &autoscaling.ResumeProcessesInput{
		AutoScalingGroupName: name,
		ScalingProcesses:     resume,
	})
	return errors.WithStackTrace(err)
}
