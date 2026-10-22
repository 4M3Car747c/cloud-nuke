package resources

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
	"github.com/gruntwork-io/cloud-nuke/config"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stopEC2Mock simulates one or more instances whose state changes with stop/start calls.
type stopEC2Mock struct {
	EC2InstancesAPI
	instances map[string]*types.Instance
	stopErr   error
	stopped   []string
	started   []string
}

func (m *stopEC2Mock) DescribeInstances(_ context.Context, in *ec2.DescribeInstancesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	var out []types.Instance
	for id, inst := range m.instances {
		if len(in.InstanceIds) > 0 && in.InstanceIds[0] != id {
			continue
		}
		if len(in.Filters) > 0 && !containsString(in.Filters[0].Values, string(inst.State.Name)) {
			continue
		}
		out = append(out, *inst)
	}
	if len(in.InstanceIds) > 0 && len(out) == 0 {
		return nil, &smithy.GenericAPIError{Code: "InvalidInstanceID.NotFound"}
	}
	return &ec2.DescribeInstancesOutput{Reservations: []types.Reservation{{Instances: out}}}, nil
}

func (m *stopEC2Mock) StopInstances(_ context.Context, in *ec2.StopInstancesInput, _ ...func(*ec2.Options)) (*ec2.StopInstancesOutput, error) {
	if m.stopErr != nil {
		return nil, m.stopErr
	}
	m.stopped = append(m.stopped, in.InstanceIds...)
	m.instances[in.InstanceIds[0]].State.Name = types.InstanceStateNameStopped
	return &ec2.StopInstancesOutput{}, nil
}

func (m *stopEC2Mock) StartInstances(_ context.Context, in *ec2.StartInstancesInput, _ ...func(*ec2.Options)) (*ec2.StartInstancesOutput, error) {
	m.started = append(m.started, in.InstanceIds...)
	m.instances[in.InstanceIds[0]].State.Name = types.InstanceStateNameRunning
	return &ec2.StartInstancesOutput{}, nil
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func ec2Inst(id string, state types.InstanceStateName, tags ...types.Tag) *types.Instance {
	return &types.Instance{
		InstanceId:     aws.String(id),
		State:          &types.InstanceState{Name: state},
		RootDeviceType: types.DeviceTypeEbs,
		Tags:           tags,
	}
}

func TestEC2Stop_ListKeepsProtectedSkipsEKSNodes(t *testing.T) {
	m := &stopEC2Mock{instances: map[string]*types.Instance{
		"i-run":  ec2Inst("i-run", types.InstanceStateNameRunning),
		"i-off":  ec2Inst("i-off", types.InstanceStateNameStopped),
		"i-node": ec2Inst("i-node", types.InstanceStateNameRunning, types.Tag{Key: aws.String("eks:nodegroup-name"), Value: aws.String("ng")}),
	}}
	ids, err := listRunningEC2Instances(context.Background(), m, resource.Scope{}, config.ResourceType{})
	require.NoError(t, err)
	assert.Equal(t, []string{"i-run"}, aws.ToStringSlice(ids))
}

func TestEC2Stop_RoundTrip(t *testing.T) {
	m := &stopEC2Mock{instances: map[string]*types.Instance{"i-1": ec2Inst("i-1", types.InstanceStateNameRunning)}}
	ctx := context.Background()
	id := aws.String("i-1")

	prev, err := captureEC2Instance(ctx, m, id)
	require.NoError(t, err)
	assert.JSONEq(t, `{"state":"running"}`, string(prev))

	require.NoError(t, stopEC2Instance(ctx, m, id, prev))
	assert.Equal(t, []string{"i-1"}, m.stopped)

	_, err = captureEC2Instance(ctx, m, id)
	assert.ErrorIs(t, err, resource.ErrAlreadyStopped, "a stopped instance has nothing to stop")

	require.NoError(t, startEC2Instance(ctx, m, id, prev))
	assert.Equal(t, []string{"i-1"}, m.started)

	// Starting a running instance again is a no-op.
	require.NoError(t, startEC2Instance(ctx, m, id, prev))
	assert.Len(t, m.started, 1)
}

func TestEC2Stop_Unsupported(t *testing.T) {
	ctx := context.Background()
	store := ec2Inst("i-store", types.InstanceStateNameRunning)
	store.RootDeviceType = types.DeviceTypeInstanceStore
	m := &stopEC2Mock{instances: map[string]*types.Instance{"i-store": store}}
	_, err := captureEC2Instance(ctx, m, aws.String("i-store"))
	assert.True(t, resource.IsUnsupported(err), err)

	spot := &stopEC2Mock{
		instances: map[string]*types.Instance{"i-spot": ec2Inst("i-spot", types.InstanceStateNameRunning)},
		stopErr:   &smithy.GenericAPIError{Code: "UnsupportedOperation", Message: "one-time Spot Instance request"},
	}
	err = stopEC2Instance(ctx, spot, aws.String("i-spot"), json.RawMessage(`{"state":"running"}`))
	assert.True(t, resource.IsUnsupported(err), err)
}

func TestEC2Stop_StartGone(t *testing.T) {
	ctx := context.Background()
	prev := json.RawMessage(`{"state":"running"}`)
	m := &stopEC2Mock{instances: map[string]*types.Instance{}}
	assert.ErrorIs(t, startEC2Instance(ctx, m, aws.String("i-missing"), prev), resource.ErrGone)

	m.instances["i-term"] = ec2Inst("i-term", types.InstanceStateNameTerminated)
	assert.ErrorIs(t, startEC2Instance(ctx, m, aws.String("i-term"), prev), resource.ErrGone)
}

func TestEC2Instances_IsStoppable(t *testing.T) {
	r, ok := NewEC2Instances().(resource.StoppableResource)
	require.True(t, ok)
	assert.True(t, r.Stoppable())
}
