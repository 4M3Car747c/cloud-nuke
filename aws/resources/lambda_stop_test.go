package resources

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/smithy-go"
	"github.com/gruntwork-io/cloud-nuke/config"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stopLambdaFn struct {
	reserved    *int32
	provisioned map[string]int32
	tags        map[string]string
}

// stopLambdaMock simulates functions with reserved and provisioned concurrency, enforcing
// Lambda's rule that reserved concurrency cannot be below the provisioned total.
type stopLambdaMock struct {
	LambdaFunctionsAPI
	fns   map[string]*stopLambdaFn
	calls []string
}

func lambdaNotFound() error { return &smithy.GenericAPIError{Code: "ResourceNotFoundException"} }

func (m *stopLambdaMock) fn(name *string) (*stopLambdaFn, error) {
	f, ok := m.fns[aws.ToString(name)]
	if !ok {
		return nil, lambdaNotFound()
	}
	return f, nil
}

func (m *stopLambdaMock) ListFunctions(_ context.Context, _ *lambda.ListFunctionsInput, _ ...func(*lambda.Options)) (*lambda.ListFunctionsOutput, error) {
	var out []types.FunctionConfiguration
	for _, name := range webSortedKeys(m.fns) {
		out = append(out, types.FunctionConfiguration{
			FunctionName: aws.String(name),
			FunctionArn:  aws.String("arn:aws:lambda:ap-northeast-1:123456789012:function:" + name),
			LastModified: aws.String(time.Now().Format(awsLambdaTimeFormat)),
		})
	}
	return &lambda.ListFunctionsOutput{Functions: out}, nil
}

func (m *stopLambdaMock) ListTags(_ context.Context, in *lambda.ListTagsInput, _ ...func(*lambda.Options)) (*lambda.ListTagsOutput, error) {
	arn := aws.ToString(in.Resource)
	name := arn[len("arn:aws:lambda:ap-northeast-1:123456789012:function:"):]
	return &lambda.ListTagsOutput{Tags: m.fns[name].tags}, nil
}

func (m *stopLambdaMock) GetFunctionConcurrency(_ context.Context, in *lambda.GetFunctionConcurrencyInput, _ ...func(*lambda.Options)) (*lambda.GetFunctionConcurrencyOutput, error) {
	f, err := m.fn(in.FunctionName)
	if err != nil {
		return nil, err
	}
	return &lambda.GetFunctionConcurrencyOutput{ReservedConcurrentExecutions: f.reserved}, nil
}

func (m *stopLambdaMock) PutFunctionConcurrency(_ context.Context, in *lambda.PutFunctionConcurrencyInput, _ ...func(*lambda.Options)) (*lambda.PutFunctionConcurrencyOutput, error) {
	f, err := m.fn(in.FunctionName)
	if err != nil {
		return nil, err
	}
	var total int32
	for _, n := range f.provisioned {
		total += n
	}
	if *in.ReservedConcurrentExecutions < total {
		return nil, &smithy.GenericAPIError{Code: "InvalidParameterValueException", Message: "reserved below provisioned"}
	}
	m.calls = append(m.calls, "put-reserved")
	f.reserved = aws.Int32(*in.ReservedConcurrentExecutions)
	return &lambda.PutFunctionConcurrencyOutput{ReservedConcurrentExecutions: f.reserved}, nil
}

func (m *stopLambdaMock) DeleteFunctionConcurrency(_ context.Context, in *lambda.DeleteFunctionConcurrencyInput, _ ...func(*lambda.Options)) (*lambda.DeleteFunctionConcurrencyOutput, error) {
	f, err := m.fn(in.FunctionName)
	if err != nil {
		return nil, err
	}
	m.calls = append(m.calls, "delete-reserved")
	f.reserved = nil
	return &lambda.DeleteFunctionConcurrencyOutput{}, nil
}

func (m *stopLambdaMock) ListProvisionedConcurrencyConfigs(_ context.Context, in *lambda.ListProvisionedConcurrencyConfigsInput, _ ...func(*lambda.Options)) (*lambda.ListProvisionedConcurrencyConfigsOutput, error) {
	f, err := m.fn(in.FunctionName)
	if err != nil {
		return nil, err
	}
	var out []types.ProvisionedConcurrencyConfigListItem
	for _, q := range webSortedKeys(f.provisioned) {
		out = append(out, types.ProvisionedConcurrencyConfigListItem{
			FunctionArn:                              aws.String("arn:aws:lambda:ap-northeast-1:123456789012:function:" + aws.ToString(in.FunctionName) + ":" + q),
			RequestedProvisionedConcurrentExecutions: aws.Int32(f.provisioned[q]),
		})
	}
	return &lambda.ListProvisionedConcurrencyConfigsOutput{ProvisionedConcurrencyConfigs: out}, nil
}

func (m *stopLambdaMock) PutProvisionedConcurrencyConfig(_ context.Context, in *lambda.PutProvisionedConcurrencyConfigInput, _ ...func(*lambda.Options)) (*lambda.PutProvisionedConcurrencyConfigOutput, error) {
	f, err := m.fn(in.FunctionName)
	if err != nil {
		return nil, err
	}
	if f.reserved != nil && *f.reserved < *in.ProvisionedConcurrentExecutions {
		return nil, &smithy.GenericAPIError{Code: "InvalidParameterValueException", Message: "provisioned above reserved"}
	}
	m.calls = append(m.calls, "put-provisioned:"+aws.ToString(in.Qualifier))
	if f.provisioned == nil {
		f.provisioned = map[string]int32{}
	}
	f.provisioned[aws.ToString(in.Qualifier)] = *in.ProvisionedConcurrentExecutions
	return &lambda.PutProvisionedConcurrencyConfigOutput{}, nil
}

func (m *stopLambdaMock) DeleteProvisionedConcurrencyConfig(_ context.Context, in *lambda.DeleteProvisionedConcurrencyConfigInput, _ ...func(*lambda.Options)) (*lambda.DeleteProvisionedConcurrencyConfigOutput, error) {
	f, err := m.fn(in.FunctionName)
	if err != nil {
		return nil, err
	}
	m.calls = append(m.calls, "delete-provisioned:"+aws.ToString(in.Qualifier))
	delete(f.provisioned, aws.ToString(in.Qualifier))
	return &lambda.DeleteProvisionedConcurrencyConfigOutput{}, nil
}

func lambdaRoundTrip(t *testing.T, m *stopLambdaMock, name, wantPrev string) {
	t.Helper()
	ctx := context.Background()
	id := aws.String(name)
	before := *m.fns[name]

	prev, err := captureLambdaFunction(ctx, m, id)
	require.NoError(t, err)
	assert.JSONEq(t, wantPrev, string(prev))

	require.NoError(t, stopLambdaFunction(ctx, m, id, prev))
	assert.Equal(t, int32(0), aws.ToInt32(m.fns[name].reserved))
	assert.Empty(t, m.fns[name].provisioned)
	require.NoError(t, stopLambdaFunction(ctx, m, id, prev), "stop is idempotent")

	_, err = captureLambdaFunction(ctx, m, id)
	assert.ErrorIs(t, err, resource.ErrAlreadyStopped)

	require.NoError(t, startLambdaFunction(ctx, m, id, prev))
	require.NoError(t, startLambdaFunction(ctx, m, id, prev), "start is idempotent")
	assert.Equal(t, before.reserved, m.fns[name].reserved)

	again, err := captureLambdaFunction(ctx, m, id)
	require.NoError(t, err)
	assert.JSONEq(t, string(prev), string(again))
}

func TestLambdaStop_RoundTripUnset(t *testing.T) {
	m := &stopLambdaMock{fns: map[string]*stopLambdaFn{"f": {}}}
	lambdaRoundTrip(t, m, "f", `{"reserved":null}`)
	assert.Equal(t, []string{"put-reserved", "put-reserved", "delete-reserved", "delete-reserved"}, m.calls)
}

func TestLambdaStop_RoundTripReserved(t *testing.T) {
	m := &stopLambdaMock{fns: map[string]*stopLambdaFn{"f": {reserved: aws.Int32(7)}}}
	lambdaRoundTrip(t, m, "f", `{"reserved":7}`)
	assert.Equal(t, int32(7), aws.ToInt32(m.fns["f"].reserved))
}

func TestLambdaStop_RoundTripProvisioned(t *testing.T) {
	m := &stopLambdaMock{fns: map[string]*stopLambdaFn{"f": {
		reserved:    aws.Int32(10),
		provisioned: map[string]int32{"live": 3, "2": 2},
	}}}
	lambdaRoundTrip(t, m, "f", `{"reserved":10,"provisioned":{"2":2,"live":3}}`)
	assert.Equal(t, map[string]int32{"live": 3, "2": 2}, m.fns["f"].provisioned)
	// Provisioned concurrency is removed before reserved goes to 0, and put back after.
	assert.Equal(t, "delete-provisioned:2", m.calls[0])
	assert.Equal(t, "put-reserved", m.calls[2])
}

func TestLambdaStop_GoneAndAlreadyStopped(t *testing.T) {
	ctx := context.Background()
	m := &stopLambdaMock{fns: map[string]*stopLambdaFn{"off": {reserved: aws.Int32(0)}}}

	_, err := captureLambdaFunction(ctx, m, aws.String("off"))
	assert.ErrorIs(t, err, resource.ErrAlreadyStopped)
	_, err = captureLambdaFunction(ctx, m, aws.String("missing"))
	assert.ErrorIs(t, err, resource.ErrAlreadyStopped)

	assert.NoError(t, stopLambdaFunction(ctx, m, aws.String("missing"), json.RawMessage(`{"reserved":null}`)))
	assert.ErrorIs(t, startLambdaFunction(ctx, m, aws.String("missing"), json.RawMessage(`{"reserved":null}`)), resource.ErrGone)
	assert.ErrorIs(t, startLambdaFunction(ctx, m, aws.String("missing"), json.RawMessage(`{"reserved":4}`)), resource.ErrGone)
}

func TestLambdaStop_ListHonoursTagFilter(t *testing.T) {
	m := &stopLambdaMock{fns: map[string]*stopLambdaFn{
		"mine":   {tags: map[string]string{"mbx-stop-test": "web"}},
		"other":  {tags: map[string]string{"mbx-stop-test": "x"}},
		"notags": {},
	}}
	r := NewLambdaFunctions().(*AwsResourceAdapter[LambdaFunctionsAPI])
	r.Resource.Client = m
	cfg := config.Config{}
	cfg.AddIncludeTags(webTagFilter("mbx-stop-test", "web").IncludeRule.Tags)
	got, err := r.ListStoppable(context.Background(), cfg)
	require.NoError(t, err)
	assert.Equal(t, []string{"mine"}, got)
}

func TestLambdaFunctions_IsStoppable(t *testing.T) {
	r, ok := NewLambdaFunctions().(resource.StoppableResource)
	require.True(t, ok)
	assert.True(t, r.Stoppable())
}
