package resources

import (
	"context"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/gruntwork-io/cloud-nuke/config"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stopASGMock keeps the suspended processes of each group.
type stopASGMock struct {
	ASGroupsAPI
	groups map[string]*types.AutoScalingGroup
}

func (m *stopASGMock) DescribeAutoScalingGroups(_ context.Context, in *autoscaling.DescribeAutoScalingGroupsInput, _ ...func(*autoscaling.Options)) (*autoscaling.DescribeAutoScalingGroupsOutput, error) {
	var out []types.AutoScalingGroup
	for name, g := range m.groups {
		if len(in.AutoScalingGroupNames) == 0 || slices.Contains(in.AutoScalingGroupNames, name) {
			out = append(out, *g)
		}
	}
	return &autoscaling.DescribeAutoScalingGroupsOutput{AutoScalingGroups: out}, nil
}

func (m *stopASGMock) SuspendProcesses(_ context.Context, in *autoscaling.SuspendProcessesInput, _ ...func(*autoscaling.Options)) (*autoscaling.SuspendProcessesOutput, error) {
	g := m.groups[aws.ToString(in.AutoScalingGroupName)]
	for _, p := range in.ScalingProcesses {
		if !slices.Contains(m.suspended(g), p) {
			g.SuspendedProcesses = append(g.SuspendedProcesses, types.SuspendedProcess{ProcessName: aws.String(p)})
		}
	}
	return &autoscaling.SuspendProcessesOutput{}, nil
}

func (m *stopASGMock) ResumeProcesses(_ context.Context, in *autoscaling.ResumeProcessesInput, _ ...func(*autoscaling.Options)) (*autoscaling.ResumeProcessesOutput, error) {
	g := m.groups[aws.ToString(in.AutoScalingGroupName)]
	g.SuspendedProcesses = slices.DeleteFunc(g.SuspendedProcesses, func(p types.SuspendedProcess) bool {
		return slices.Contains(in.ScalingProcesses, aws.ToString(p.ProcessName))
	})
	return &autoscaling.ResumeProcessesOutput{}, nil
}

func (m *stopASGMock) suspended(g *types.AutoScalingGroup) []string {
	var out []string
	for _, p := range g.SuspendedProcesses {
		out = append(out, aws.ToString(p.ProcessName))
	}
	slices.Sort(out)
	return out
}

func asgGroup(name string, suspended ...string) *types.AutoScalingGroup {
	g := &types.AutoScalingGroup{AutoScalingGroupName: aws.String(name)}
	for _, p := range suspended {
		g.SuspendedProcesses = append(g.SuspendedProcesses, types.SuspendedProcess{ProcessName: aws.String(p)})
	}
	return g
}

func TestASGStop_ListSkipsEKSNodegroups(t *testing.T) {
	eks := asgGroup("eks-ng")
	eks.Tags = []types.TagDescription{{Key: aws.String("eks:nodegroup-name"), Value: aws.String("ng")}}
	m := &stopASGMock{groups: map[string]*types.AutoScalingGroup{"web": asgGroup("web"), "eks-ng": eks}}
	names, err := listStoppableASGroups(context.Background(), m, resource.Scope{}, config.ResourceType{})
	require.NoError(t, err)
	assert.Equal(t, []string{"web"}, aws.ToStringSlice(names))
}

func TestASGStop_RoundTripKeepsCustomerSuspensions(t *testing.T) {
	ctx := context.Background()
	// The customer had already suspended AZRebalance and an unrelated process.
	m := &stopASGMock{groups: map[string]*types.AutoScalingGroup{"web": asgGroup("web", "AZRebalance", "AddToLoadBalancer")}}
	name := aws.String("web")

	prev, err := captureASG(ctx, m, name)
	require.NoError(t, err)
	assert.JSONEq(t, `{"suspended":["AZRebalance"]}`, string(prev))

	require.NoError(t, stopASG(ctx, m, name, prev))
	_, err = captureASG(ctx, m, name)
	assert.ErrorIs(t, err, resource.ErrAlreadyStopped)

	require.NoError(t, startASG(ctx, m, name, prev))
	assert.Equal(t, []string{"AZRebalance", "AddToLoadBalancer"}, m.suspended(m.groups["web"]))
}

func TestASGStop_StartGone(t *testing.T) {
	m := &stopASGMock{groups: map[string]*types.AutoScalingGroup{}}
	err := startASG(context.Background(), m, aws.String("missing"), []byte(`{"suspended":[]}`))
	assert.ErrorIs(t, err, resource.ErrGone)
}
