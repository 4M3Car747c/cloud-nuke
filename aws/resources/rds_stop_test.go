package resources

import (
	"context"
	"encoding/json"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/smithy-go"
	"github.com/gruntwork-io/cloud-nuke/config"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rdsTransitions is how the mocks move a resource on after each describe, like AWS would.
var rdsTransitions = map[string]string{
	"stopping":  "stopped",
	"starting":  "available",
	"modifying": "available",
	"creating":  "available",
}

func fastRDSPolling(t *testing.T) {
	old := rdsPollInterval
	rdsPollInterval = time.Millisecond
	t.Cleanup(func() { rdsPollInterval = old })
}

// stopRDSInstanceMock simulates DB instances whose status changes with stop/start calls.
type stopRDSInstanceMock struct {
	DBInstancesAPI
	instances map[string]*types.DBInstance
	stopErr   error
	stopped   []string
	started   []string
}

func (m *stopRDSInstanceMock) DescribeDBInstances(_ context.Context, in *rds.DescribeDBInstancesInput, _ ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error) {
	var ids []string
	for id := range m.instances {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var out []types.DBInstance
	for _, id := range ids {
		if in.DBInstanceIdentifier != nil && *in.DBInstanceIdentifier != id {
			continue
		}
		inst := m.instances[id]
		out = append(out, *inst)
		if next, ok := rdsTransitions[aws.ToString(inst.DBInstanceStatus)]; ok {
			inst.DBInstanceStatus = aws.String(next)
		}
	}
	if in.DBInstanceIdentifier != nil && len(out) == 0 {
		return nil, &types.DBInstanceNotFoundFault{Message: aws.String("not found")}
	}
	return &rds.DescribeDBInstancesOutput{DBInstances: out}, nil
}

func (m *stopRDSInstanceMock) StopDBInstance(_ context.Context, in *rds.StopDBInstanceInput, _ ...func(*rds.Options)) (*rds.StopDBInstanceOutput, error) {
	if m.stopErr != nil {
		return nil, m.stopErr
	}
	id := aws.ToString(in.DBInstanceIdentifier)
	m.stopped = append(m.stopped, id)
	m.instances[id].DBInstanceStatus = aws.String("stopping")
	return &rds.StopDBInstanceOutput{}, nil
}

func (m *stopRDSInstanceMock) StartDBInstance(_ context.Context, in *rds.StartDBInstanceInput, _ ...func(*rds.Options)) (*rds.StartDBInstanceOutput, error) {
	id := aws.ToString(in.DBInstanceIdentifier)
	m.started = append(m.started, id)
	m.instances[id].DBInstanceStatus = aws.String("starting")
	return &rds.StartDBInstanceOutput{}, nil
}

func rdsInst(id, status string) *types.DBInstance {
	return &types.DBInstance{
		DBInstanceIdentifier: aws.String(id),
		DBInstanceStatus:     aws.String(status),
		Engine:               aws.String("postgres"),
		TagList:              []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	}
}

func TestRDSInstanceStop_ListSkipsClusterMembers(t *testing.T) {
	member := rdsInst("db-member", "available")
	member.DBClusterIdentifier = aws.String("cl")
	other := rdsInst("db-other", "available")
	other.TagList = nil
	m := &stopRDSInstanceMock{instances: map[string]*types.DBInstance{
		"db-a":      rdsInst("db-a", "available"),
		"db-off":    rdsInst("db-off", "stopped"),
		"db-member": member,
		"db-other":  other,
	}}
	ids, err := listStoppableDBInstances(context.Background(), m, resource.Scope{}, config.ResourceType{})
	require.NoError(t, err)
	assert.Equal(t, []string{"db-a", "db-off", "db-other"}, aws.ToStringSlice(ids))

	cfg := config.Config{}
	cfg.AddIncludeTags(map[string]config.Expression{"env": {RE: *regexp.MustCompile("^test$")}})
	ids, err = listStoppableDBInstances(context.Background(), m, resource.Scope{}, cfg.DBInstances.ResourceType)
	require.NoError(t, err)
	assert.Equal(t, []string{"db-a", "db-off"}, aws.ToStringSlice(ids))
}

func TestRDSInstanceStop_RoundTrip(t *testing.T) {
	fastRDSPolling(t)
	m := &stopRDSInstanceMock{instances: map[string]*types.DBInstance{"db-1": rdsInst("db-1", "available")}}
	ctx := context.Background()
	id := aws.String("db-1")

	prev, err := captureDBInstance(ctx, m, id)
	require.NoError(t, err)
	assert.JSONEq(t, `{"status":"available"}`, string(prev))

	require.NoError(t, stopDBInstance(ctx, m, id, prev))
	assert.Equal(t, []string{"db-1"}, m.stopped)

	// Stopping again (crash and rerun) is a no-op.
	require.NoError(t, stopDBInstance(ctx, m, id, prev))
	assert.Len(t, m.stopped, 1)

	_, err = captureDBInstance(ctx, m, id)
	assert.ErrorIs(t, err, resource.ErrAlreadyStopped)

	require.NoError(t, startDBInstance(ctx, m, id, prev))
	assert.Equal(t, []string{"db-1"}, m.started)

	// Capture waits out "starting" and then matches the original.
	again, err := captureDBInstance(ctx, m, id)
	require.NoError(t, err)
	assert.JSONEq(t, string(prev), string(again))

	require.NoError(t, startDBInstance(ctx, m, id, prev))
	assert.Len(t, m.started, 1, "starting an available instance is a no-op")
}

func TestRDSInstanceStop_StartWhileStopping(t *testing.T) {
	fastRDSPolling(t)
	m := &stopRDSInstanceMock{instances: map[string]*types.DBInstance{"db-1": rdsInst("db-1", "stopping")}}
	require.NoError(t, startDBInstance(context.Background(), m, aws.String("db-1"), json.RawMessage(`{"status":"available"}`)))
	assert.Equal(t, []string{"db-1"}, m.started)
}

func TestRDSInstanceStop_CaptureStates(t *testing.T) {
	fastRDSPolling(t)
	ctx := context.Background()
	for status, want := range map[string]error{
		"stopped":  resource.ErrAlreadyStopped,
		"stopping": resource.ErrAlreadyStopped,
		"deleting": resource.ErrAlreadyStopped,
	} {
		m := &stopRDSInstanceMock{instances: map[string]*types.DBInstance{"db": rdsInst("db", status)}}
		_, err := captureDBInstance(ctx, m, aws.String("db"))
		assert.ErrorIs(t, err, want, status)
	}

	m := &stopRDSInstanceMock{instances: map[string]*types.DBInstance{}}
	_, err := captureDBInstance(ctx, m, aws.String("missing"))
	assert.ErrorIs(t, err, resource.ErrAlreadyStopped)

	m = &stopRDSInstanceMock{instances: map[string]*types.DBInstance{"db": rdsInst("db", "modifying")}}
	prev, err := captureDBInstance(ctx, m, aws.String("db"))
	require.NoError(t, err, "modifying is waited out")
	assert.JSONEq(t, `{"status":"available"}`, string(prev))

	for _, broken := range []string{"failed", "storage-full", "incompatible-parameters", "inaccessible-encryption-credentials"} {
		m = &stopRDSInstanceMock{instances: map[string]*types.DBInstance{"db": rdsInst("db", broken)}}
		_, err = captureDBInstance(ctx, m, aws.String("db"))
		require.Error(t, err, broken)
		assert.False(t, resource.IsUnsupported(err), broken)
		assert.NotErrorIs(t, err, resource.ErrAlreadyStopped, broken)
	}
}

func TestRDSInstanceStop_Unsupported(t *testing.T) {
	ctx := context.Background()
	replica := rdsInst("db-replica", "available")
	replica.ReadReplicaSourceDBInstanceIdentifier = aws.String("db-src")
	source := rdsInst("db-src", "available")
	source.ReadReplicaDBInstanceIdentifiers = []string{"db-replica"}
	sqlserver := rdsInst("db-mssql", "available")
	sqlserver.Engine = aws.String("sqlserver-se")
	sqlserver.MultiAZ = aws.Bool(true)
	member := rdsInst("db-member", "available")
	member.DBClusterIdentifier = aws.String("cl")
	m := &stopRDSInstanceMock{instances: map[string]*types.DBInstance{
		"db-replica": replica, "db-src": source, "db-mssql": sqlserver, "db-member": member,
	}}
	for id := range m.instances {
		_, err := captureDBInstance(ctx, m, aws.String(id))
		assert.True(t, resource.IsUnsupported(err), "%s: %v", id, err)
	}

	m = &stopRDSInstanceMock{
		instances: map[string]*types.DBInstance{"db": rdsInst("db", "available")},
		stopErr:   &smithy.GenericAPIError{Code: "InvalidParameterCombination", Message: "Cannot stop or start a SQLServer Multi-AZ database instance"},
	}
	err := stopDBInstance(ctx, m, aws.String("db"), json.RawMessage(`{"status":"available"}`))
	assert.True(t, resource.IsUnsupported(err), err)

	// Real messages from StopDBInstance (us-west-2, 2026-10).
	for _, msg := range []string{
		"Cannot stop or start the source instance for a Read-Replica",
		"Cannot stop or start a Read-Replica target instance",
	} {
		m.stopErr = &types.InvalidDBInstanceStateFault{Message: aws.String(msg)}
		err = stopDBInstance(ctx, m, aws.String("db"), json.RawMessage(`{"status":"available"}`))
		assert.True(t, resource.IsUnsupported(err), err)
	}

	// A transient state error is a plain failure, to be retried.
	m.stopErr = &types.InvalidDBInstanceStateFault{Message: aws.String("Instance db is not in available state.")}
	err = stopDBInstance(ctx, m, aws.String("db"), json.RawMessage(`{"status":"available"}`))
	require.Error(t, err)
	assert.False(t, resource.IsUnsupported(err), err)
}

func TestRDSInstanceStop_StartGone(t *testing.T) {
	ctx := context.Background()
	prev := json.RawMessage(`{"status":"available"}`)
	m := &stopRDSInstanceMock{instances: map[string]*types.DBInstance{}}
	assert.ErrorIs(t, startDBInstance(ctx, m, aws.String("missing"), prev), resource.ErrGone)

	m.instances["db-del"] = rdsInst("db-del", "deleting")
	assert.ErrorIs(t, startDBInstance(ctx, m, aws.String("db-del"), prev), resource.ErrGone)
}

func TestDBInstances_IsStoppable(t *testing.T) {
	r, ok := NewDBInstances().(resource.StoppableResource)
	require.True(t, ok)
	assert.True(t, r.Stoppable())
}
