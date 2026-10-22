package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront"
	cftypes "github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/aws/smithy-go"
	"github.com/gruntwork-io/cloud-nuke/config"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stopCFDist struct {
	enabled bool
	staging bool
	tags    map[string]string
	etag    int
}

// stopCFMock simulates distributions whose Enabled flag and ETag change with updates.
type stopCFMock struct {
	CloudfrontDistributionAPI
	dists map[string]*stopCFDist
	// staleOnce makes the next UpdateDistribution fail with PreconditionFailed (as if
	// someone else updated the distribution in between) this many times.
	staleOnce int
	updates   []bool
	tagErr    error
}

func cfNotFound() error { return &smithy.GenericAPIError{Code: "NoSuchDistribution"} }

func (m *stopCFMock) ListDistributions(_ context.Context, _ *cloudfront.ListDistributionsInput, _ ...func(*cloudfront.Options)) (*cloudfront.ListDistributionsOutput, error) {
	var items []cftypes.DistributionSummary
	for _, id := range webSortedKeys(m.dists) {
		d := m.dists[id]
		items = append(items, cftypes.DistributionSummary{
			Id:      aws.String(id),
			ARN:     aws.String("arn:aws:cloudfront::123456789012:distribution/" + id),
			Enabled: aws.Bool(d.enabled),
			Staging: aws.Bool(d.staging),
		})
	}
	return &cloudfront.ListDistributionsOutput{DistributionList: &cftypes.DistributionList{Items: items}}, nil
}

func (m *stopCFMock) ListTagsForResource(_ context.Context, in *cloudfront.ListTagsForResourceInput, _ ...func(*cloudfront.Options)) (*cloudfront.ListTagsForResourceOutput, error) {
	if m.tagErr != nil {
		return nil, m.tagErr
	}
	arn := aws.ToString(in.Resource)
	id := arn[len("arn:aws:cloudfront::123456789012:distribution/"):]
	var items []cftypes.Tag
	for k, v := range m.dists[id].tags {
		items = append(items, cftypes.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	return &cloudfront.ListTagsForResourceOutput{Tags: &cftypes.Tags{Items: items}}, nil
}

func (m *stopCFMock) GetDistributionConfig(_ context.Context, in *cloudfront.GetDistributionConfigInput, _ ...func(*cloudfront.Options)) (*cloudfront.GetDistributionConfigOutput, error) {
	d, ok := m.dists[aws.ToString(in.Id)]
	if !ok {
		return nil, cfNotFound()
	}
	return &cloudfront.GetDistributionConfigOutput{
		ETag: aws.String(fmt.Sprintf("E%d", d.etag)),
		DistributionConfig: &cftypes.DistributionConfig{
			Enabled: aws.Bool(d.enabled),
			Comment: aws.String("keep me"),
		},
	}, nil
}

func (m *stopCFMock) UpdateDistribution(_ context.Context, in *cloudfront.UpdateDistributionInput, _ ...func(*cloudfront.Options)) (*cloudfront.UpdateDistributionOutput, error) {
	d, ok := m.dists[aws.ToString(in.Id)]
	if !ok {
		return nil, cfNotFound()
	}
	if m.staleOnce > 0 {
		m.staleOnce--
		d.etag++
	}
	if aws.ToString(in.IfMatch) != fmt.Sprintf("E%d", d.etag) {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed"}
	}
	if aws.ToString(in.DistributionConfig.Comment) != "keep me" {
		return nil, fmt.Errorf("config was not round-tripped")
	}
	d.enabled = aws.ToBool(in.DistributionConfig.Enabled)
	d.etag++
	m.updates = append(m.updates, d.enabled)
	return &cloudfront.UpdateDistributionOutput{}, nil
}

func webSortedKeys[V any](m map[string]V) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	for i := range keys {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}

func webTagFilter(key, value string) config.ResourceType {
	return config.ResourceType{IncludeRule: config.FilterRule{Tags: map[string]config.Expression{
		key: {RE: *regexp.MustCompile("^" + regexp.QuoteMeta(value) + "$")},
	}}}
}

func TestCloudfrontStop_ListEnabledAndTagFilter(t *testing.T) {
	m := &stopCFMock{dists: map[string]*stopCFDist{
		"EFS4VWBNKD6XB": {enabled: true}, // untagged, unrelated
		"EOTHER":        {enabled: true, tags: map[string]string{"mbx-stop-test": "other"}},
		"EMINE":         {enabled: true, tags: map[string]string{"mbx-stop-test": "web"}},
		"EMINEOFF":      {enabled: false, tags: map[string]string{"mbx-stop-test": "web"}},
		"EMINESTAGE":    {enabled: true, staging: true, tags: map[string]string{"mbx-stop-test": "web"}},
	}}
	ctx := context.Background()

	ids, err := listEnabledCloudfrontDistributions(ctx, m, resource.Scope{}, webTagFilter("mbx-stop-test", "web"))
	require.NoError(t, err)
	assert.Equal(t, []string{"EMINE"}, aws.ToStringSlice(ids), "the tag filter must keep unrelated distributions out")

	ids, err = listEnabledCloudfrontDistributions(ctx, m, resource.Scope{}, config.ResourceType{})
	require.NoError(t, err)
	assert.Equal(t, []string{"EFS4VWBNKD6XB", "EMINE", "EOTHER"}, aws.ToStringSlice(ids), "without a filter every enabled, non-staging distribution")

	// The same filter through the framework, as the live test and StopAllResources use it.
	r := NewCloudfrontDistributions().(*AwsResourceAdapter[CloudfrontDistributionAPI])
	r.Resource.Client = m
	cfg := config.Config{}
	cfg.AddIncludeTags(map[string]config.Expression{"mbx-stop-test": {RE: *regexp.MustCompile("^web$")}})
	got, err := r.ListStoppable(ctx, cfg)
	require.NoError(t, err)
	assert.Equal(t, []string{"EMINE"}, got)

	m.tagErr = &smithy.GenericAPIError{Code: "AccessDenied"}
	_, err = listEnabledCloudfrontDistributions(ctx, m, resource.Scope{}, config.ResourceType{})
	assert.Error(t, err, "a tag lookup failure must not silently skip a distribution")
}

func TestCloudfrontStop_RoundTrip(t *testing.T) {
	m := &stopCFMock{dists: map[string]*stopCFDist{"E1": {enabled: true}}}
	ctx := context.Background()
	id := aws.String("E1")

	prev, err := captureCloudfrontDistribution(ctx, m, id)
	require.NoError(t, err)
	assert.JSONEq(t, `{"enabled":true}`, string(prev))

	require.NoError(t, stopCloudfrontDistribution(ctx, m, id, prev))
	assert.False(t, m.dists["E1"].enabled)
	// Stopping again is a no-op.
	require.NoError(t, stopCloudfrontDistribution(ctx, m, id, prev))
	assert.Equal(t, []bool{false}, m.updates)

	_, err = captureCloudfrontDistribution(ctx, m, id)
	assert.ErrorIs(t, err, resource.ErrAlreadyStopped)

	require.NoError(t, startCloudfrontDistribution(ctx, m, id, prev))
	require.NoError(t, startCloudfrontDistribution(ctx, m, id, prev))
	assert.Equal(t, []bool{false, true}, m.updates)

	again, err := captureCloudfrontDistribution(ctx, m, id)
	require.NoError(t, err)
	assert.JSONEq(t, string(prev), string(again))
}

func TestCloudfrontStop_RetriesStaleETagOnce(t *testing.T) {
	ctx := context.Background()
	prev := json.RawMessage(`{"enabled":true}`)

	m := &stopCFMock{dists: map[string]*stopCFDist{"E1": {enabled: true}}, staleOnce: 1}
	require.NoError(t, stopCloudfrontDistribution(ctx, m, aws.String("E1"), prev))
	assert.False(t, m.dists["E1"].enabled)

	m = &stopCFMock{dists: map[string]*stopCFDist{"E1": {enabled: true}}, staleOnce: 2}
	err := stopCloudfrontDistribution(ctx, m, aws.String("E1"), prev)
	assert.True(t, hasErrorCode(err, "PreconditionFailed"), err)
}

func TestCloudfrontStop_AlreadyStoppedAndGone(t *testing.T) {
	ctx := context.Background()
	prev := json.RawMessage(`{"enabled":true}`)
	m := &stopCFMock{dists: map[string]*stopCFDist{"EOFF": {enabled: false}}}

	_, err := captureCloudfrontDistribution(ctx, m, aws.String("EOFF"))
	assert.ErrorIs(t, err, resource.ErrAlreadyStopped)
	_, err = captureCloudfrontDistribution(ctx, m, aws.String("EMISSING"))
	assert.ErrorIs(t, err, resource.ErrAlreadyStopped)

	assert.NoError(t, stopCloudfrontDistribution(ctx, m, aws.String("EMISSING"), prev))
	assert.ErrorIs(t, startCloudfrontDistribution(ctx, m, aws.String("EMISSING"), prev), resource.ErrGone)
	assert.Empty(t, m.updates)
}

func TestCloudfrontDistributions_IsStoppable(t *testing.T) {
	r, ok := NewCloudfrontDistributions().(resource.StoppableResource)
	require.True(t, ok)
	assert.True(t, r.Stoppable())
}
