package resources

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/applicationautoscaling"
	aastypes "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling/types"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/smithy-go"
	"github.com/gruntwork-io/cloud-nuke/config"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ecsTestCluster = "arn:aws:ecs:ap-northeast-1:123456789012:cluster/c1"
	ecsTestService = "arn:aws:ecs:ap-northeast-1:123456789012:service/c1/web"
)

// stopECSMock simulates services in clusters keyed by cluster ARN, then service ARN.
type stopECSMock struct {
	ECSServicesAPI
	clusters map[string]map[string]*types.Service
	updates  []int32
	// targets are Application Auto Scaling targets keyed by resource ID.
	targets map[string]*aastypes.ScalableTarget
	// order logs "scale:<n>" and "suspend:<in>/<out>/<sched>" calls in sequence.
	order []string
}

func (m *stopECSMock) DescribeScalableTargets(_ context.Context, in *applicationautoscaling.DescribeScalableTargetsInput, _ ...func(*applicationautoscaling.Options)) (*applicationautoscaling.DescribeScalableTargetsOutput, error) {
	out := &applicationautoscaling.DescribeScalableTargetsOutput{}
	for _, id := range in.ResourceIds {
		if t, ok := m.targets[id]; ok {
			out.ScalableTargets = append(out.ScalableTargets, *t)
		}
	}
	return out, nil
}

func (m *stopECSMock) RegisterScalableTarget(_ context.Context, in *applicationautoscaling.RegisterScalableTargetInput, _ ...func(*applicationautoscaling.Options)) (*applicationautoscaling.RegisterScalableTargetOutput, error) {
	t, ok := m.targets[aws.ToString(in.ResourceId)]
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "ValidationException"}
	}
	s := in.SuspendedState
	t.SuspendedState = s
	m.order = append(m.order, "suspend:"+fmtBool(*s.DynamicScalingInSuspended)+"/"+fmtBool(*s.DynamicScalingOutSuspended)+"/"+fmtBool(*s.ScheduledScalingSuspended))
	return &applicationautoscaling.RegisterScalableTargetOutput{}, nil
}

func fmtBool(b bool) string {
	if b {
		return "t"
	}
	return "f"
}

func ecsSvc(cluster, arn string, desired int32, tags map[string]string) *types.Service {
	var t []types.Tag
	for k, v := range tags {
		t = append(t, types.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	return &types.Service{
		ClusterArn:         aws.String(cluster),
		ServiceArn:         aws.String(arn),
		ServiceName:        aws.String(arn[len(arn)-3:]),
		Status:             aws.String("ACTIVE"),
		DesiredCount:       desired,
		SchedulingStrategy: types.SchedulingStrategyReplica,
		Tags:               t,
	}
}

func (m *stopECSMock) ListClusters(_ context.Context, _ *ecs.ListClustersInput, _ ...func(*ecs.Options)) (*ecs.ListClustersOutput, error) {
	return &ecs.ListClustersOutput{ClusterArns: webSortedKeys(m.clusters)}, nil
}

func (m *stopECSMock) ListServices(_ context.Context, in *ecs.ListServicesInput, _ ...func(*ecs.Options)) (*ecs.ListServicesOutput, error) {
	return &ecs.ListServicesOutput{ServiceArns: webSortedKeys(m.clusters[aws.ToString(in.Cluster)])}, nil
}

func (m *stopECSMock) DescribeServices(_ context.Context, in *ecs.DescribeServicesInput, _ ...func(*ecs.Options)) (*ecs.DescribeServicesOutput, error) {
	c, ok := m.clusters[aws.ToString(in.Cluster)]
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "ClusterNotFoundException"}
	}
	out := &ecs.DescribeServicesOutput{}
	for _, s := range in.Services {
		if svc, ok := c[s]; ok {
			out.Services = append(out.Services, *svc)
		} else {
			out.Failures = append(out.Failures, types.Failure{Arn: aws.String(s), Reason: aws.String("MISSING")})
		}
	}
	return out, nil
}

func (m *stopECSMock) UpdateService(_ context.Context, in *ecs.UpdateServiceInput, _ ...func(*ecs.Options)) (*ecs.UpdateServiceOutput, error) {
	c, ok := m.clusters[aws.ToString(in.Cluster)]
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "ClusterNotFoundException"}
	}
	svc, ok := c[aws.ToString(in.Service)]
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "ServiceNotFoundException"}
	}
	if aws.ToString(svc.Status) != "ACTIVE" {
		return nil, &smithy.GenericAPIError{Code: "ServiceNotActiveException"}
	}
	svc.DesiredCount = aws.ToInt32(in.DesiredCount)
	m.updates = append(m.updates, svc.DesiredCount)
	m.order = append(m.order, "scale:"+string(rune('0'+svc.DesiredCount)))
	return &ecs.UpdateServiceOutput{Service: svc}, nil
}

func TestECSClusterFromServiceArn(t *testing.T) {
	assert.Equal(t, ecsTestCluster, ecsClusterFromServiceArn(ecsTestService))
	assert.Equal(t, "", ecsClusterFromServiceArn("arn:aws:ecs:ap-northeast-1:123456789012:service/web"), "old ARN format has no cluster")
	assert.Equal(t, "", ecsClusterFromServiceArn("web"))
}

func TestECSServiceStop_RoundTrip(t *testing.T) {
	m := &stopECSMock{clusters: map[string]map[string]*types.Service{
		ecsTestCluster: {ecsTestService: ecsSvc(ecsTestCluster, ecsTestService, 3, nil)},
	}}
	ctx := context.Background()
	id := aws.String(ecsTestService)

	// No lister map: the cluster comes from the ARN.
	prev, err := captureECSService(ctx, m, id, nil)
	require.NoError(t, err)
	assert.JSONEq(t, `{"cluster":"`+ecsTestCluster+`","desired_count":3}`, string(prev))

	require.NoError(t, stopECSService(ctx, m, id, prev))
	require.NoError(t, stopECSService(ctx, m, id, prev))
	assert.Equal(t, int32(0), m.clusters[ecsTestCluster][ecsTestService].DesiredCount)

	_, err = captureECSService(ctx, m, id, nil)
	assert.ErrorIs(t, err, resource.ErrAlreadyStopped)

	require.NoError(t, startECSService(ctx, m, id, prev))
	require.NoError(t, startECSService(ctx, m, id, prev), "start is idempotent")
	assert.Equal(t, []int32{0, 0, 3}, m.updates)

	again, err := captureECSService(ctx, m, id, nil)
	require.NoError(t, err)
	assert.JSONEq(t, string(prev), string(again))
}

func TestECSServiceStop_OldArnUsesListerMap(t *testing.T) {
	old := "arn:aws:ecs:ap-northeast-1:123456789012:service/web"
	m := &stopECSMock{clusters: map[string]map[string]*types.Service{
		ecsTestCluster: {old: ecsSvc(ecsTestCluster, old, 2, nil)},
	}}
	ctx := context.Background()
	_, err := captureECSService(ctx, m, aws.String(old), nil)
	assert.Error(t, err, "no cluster known for an old-format ARN")

	prev, err := captureECSService(ctx, m, aws.String(old), func(string) string { return ecsTestCluster })
	require.NoError(t, err)
	assert.JSONEq(t, `{"cluster":"`+ecsTestCluster+`","desired_count":2}`, string(prev))
}

func TestECSServiceStop_AlreadyStoppedDaemonAndGone(t *testing.T) {
	daemonArn := "arn:aws:ecs:ap-northeast-1:123456789012:service/c1/dmn"
	zeroArn := "arn:aws:ecs:ap-northeast-1:123456789012:service/c1/off"
	drainArn := "arn:aws:ecs:ap-northeast-1:123456789012:service/c1/drn"
	daemon := ecsSvc(ecsTestCluster, daemonArn, 2, nil)
	daemon.SchedulingStrategy = types.SchedulingStrategyDaemon
	draining := ecsSvc(ecsTestCluster, drainArn, 1, nil)
	draining.Status = aws.String("DRAINING")
	m := &stopECSMock{clusters: map[string]map[string]*types.Service{
		ecsTestCluster: {
			daemonArn: daemon,
			zeroArn:   ecsSvc(ecsTestCluster, zeroArn, 0, nil),
			drainArn:  draining,
		},
	}}
	ctx := context.Background()
	for _, arn := range []string{daemonArn, zeroArn, drainArn, "arn:aws:ecs:ap-northeast-1:123456789012:service/c1/gone", "arn:aws:ecs:ap-northeast-1:123456789012:service/nocluster/x"} {
		_, err := captureECSService(ctx, m, aws.String(arn), nil)
		assert.ErrorIs(t, err, resource.ErrAlreadyStopped, arn)
	}

	prev := json.RawMessage(`{"cluster":"` + ecsTestCluster + `","desired_count":1}`)
	assert.NoError(t, stopECSService(ctx, m, aws.String(drainArn), prev))
	assert.ErrorIs(t, startECSService(ctx, m, aws.String(drainArn), prev), resource.ErrGone)
	assert.ErrorIs(t, startECSService(ctx, m, aws.String("arn:aws:ecs:ap-northeast-1:123456789012:service/c1/gone"), prev), resource.ErrGone)
	gonePrev := json.RawMessage(`{"cluster":"arn:aws:ecs:ap-northeast-1:123456789012:cluster/deleted","desired_count":1}`)
	assert.ErrorIs(t, startECSService(ctx, m, aws.String("arn:aws:ecs:ap-northeast-1:123456789012:service/deleted/x"), gonePrev), resource.ErrGone)
	assert.Empty(t, m.updates)
}

func TestECSServices_StoppableThroughFramework(t *testing.T) {
	other := "arn:aws:ecs:ap-northeast-1:123456789012:service/c1/oth"
	m := &stopECSMock{clusters: map[string]map[string]*types.Service{
		ecsTestCluster: {
			ecsTestService: ecsSvc(ecsTestCluster, ecsTestService, 1, map[string]string{"mbx-stop-test": "web"}),
			other:          ecsSvc(ecsTestCluster, other, 1, nil),
		},
	}}
	r := NewECSServices().(*AwsResourceAdapter[ECSServicesAPI])
	require.True(t, r.Stoppable())
	r.Resource.Client = m

	cfg := config.Config{}
	cfg.AddIncludeTags(webTagFilter("mbx-stop-test", "web").IncludeRule.Tags)
	ids, err := r.ListStoppable(context.Background(), cfg)
	require.NoError(t, err)
	assert.Equal(t, []string{ecsTestService}, ids)

	prev, err := r.CaptureState(context.Background(), ecsTestService)
	require.NoError(t, err)
	assert.JSONEq(t, `{"cluster":"`+ecsTestCluster+`","desired_count":1}`, string(prev))
}

func TestECSServiceStop_SuspendsAutoScaling(t *testing.T) {
	ctx := context.Background()
	m := &stopECSMock{
		clusters: map[string]map[string]*types.Service{
			ecsTestCluster: {ecsTestService: ecsSvc(ecsTestCluster, ecsTestService, 3, nil)},
		},
		targets: map[string]*aastypes.ScalableTarget{
			// The customer had already paused scheduled scaling.
			"service/c1/web": {
				ResourceId: aws.String("service/c1/web"),
				SuspendedState: &aastypes.SuspendedState{
					DynamicScalingInSuspended:  aws.Bool(false),
					DynamicScalingOutSuspended: aws.Bool(false),
					ScheduledScalingSuspended:  aws.Bool(true),
				},
			},
		},
	}
	id := aws.String(ecsTestService)

	prev, err := captureECSService(ctx, m, id, nil)
	require.NoError(t, err)
	assert.JSONEq(t, `{"cluster":"`+ecsTestCluster+`","desired_count":3,
		"scaling":{"dynamic_in":false,"dynamic_out":false,"scheduled":true}}`, string(prev))

	require.NoError(t, stopECSService(ctx, m, id, prev))
	// Auto scaling is paused before the service is scaled to zero.
	assert.Equal(t, []string{"suspend:t/t/t", "scale:0"}, m.order)

	_, err = captureECSService(ctx, m, id, nil)
	assert.ErrorIs(t, err, resource.ErrAlreadyStopped)

	m.order = nil
	require.NoError(t, startECSService(ctx, m, id, prev))
	assert.Equal(t, []string{"scale:3", "suspend:f/f/t"}, m.order, "desired count first, then the customer's own suspension state")

	again, err := captureECSService(ctx, m, id, nil)
	require.NoError(t, err)
	assert.JSONEq(t, string(prev), string(again))

	// Auto scaling removed while stopped: start still restores the desired count.
	m.order = nil
	m.clusters[ecsTestCluster][ecsTestService].DesiredCount = 0
	delete(m.targets, "service/c1/web")
	require.NoError(t, startECSService(ctx, m, id, prev))
	assert.Equal(t, []string{"scale:3"}, m.order)
}
