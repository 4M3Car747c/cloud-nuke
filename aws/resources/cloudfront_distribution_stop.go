package resources

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/gruntwork-io/cloud-nuke/config"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/gruntwork-io/cloud-nuke/util"
	"github.com/gruntwork-io/go-commons/errors"
)

// cloudfrontPrevState is what Start needs to restore a distribution.
type cloudfrontPrevState struct {
	Enabled bool `json:"enabled"`
}

func cloudfrontDistributionStopHooks() *resource.StopHooks[CloudfrontDistributionAPI] {
	return &resource.StopHooks[CloudfrontDistributionAPI]{
		List:    listEnabledCloudfrontDistributions,
		Capture: captureCloudfrontDistribution,
		Stop:    stopCloudfrontDistribution,
		Start:   startCloudfrontDistribution,
	}
}

// listEnabledCloudfrontDistributions lists enabled distributions that match cfg. Disabled
// distributions serve nothing, and staging distributions only serve through their primary.
// Unlike the nuke lister, a tag lookup failure is an error: skipping the distribution would
// leave it serving (and billing) without anyone noticing.
func listEnabledCloudfrontDistributions(ctx context.Context, client CloudfrontDistributionAPI, _ resource.Scope, cfg config.ResourceType) ([]*string, error) {
	var ids []*string
	paginator := cloudfront.NewListDistributionsPaginator(client, &cloudfront.ListDistributionsInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, errors.WithStackTrace(err)
		}
		if page.DistributionList == nil {
			continue
		}
		for _, item := range page.DistributionList.Items {
			if !aws.ToBool(item.Enabled) || aws.ToBool(item.Staging) {
				continue
			}
			out, err := client.ListTagsForResource(ctx, &cloudfront.ListTagsForResourceInput{Resource: item.ARN})
			if err != nil {
				return nil, errors.WithStackTrace(err)
			}
			tags := map[string]string{}
			if out != nil && out.Tags != nil {
				tags = util.ConvertCloudFrontTagsToMap(out.Tags.Items)
			}
			if cfg.ShouldInclude(config.ResourceValue{Name: item.Id, Tags: tags}) {
				ids = append(ids, item.Id)
			}
		}
	}
	return ids, nil
}

func captureCloudfrontDistribution(ctx context.Context, client CloudfrontDistributionAPI, id *string) (json.RawMessage, error) {
	out, err := client.GetDistributionConfig(ctx, &cloudfront.GetDistributionConfigInput{Id: id})
	if err != nil {
		if hasErrorCode(err, "NoSuchDistribution") {
			return nil, resource.ErrAlreadyStopped
		}
		return nil, errors.WithStackTrace(err)
	}
	if out.DistributionConfig == nil || !aws.ToBool(out.DistributionConfig.Enabled) {
		return nil, resource.ErrAlreadyStopped
	}
	return marshalPrev(cloudfrontPrevState{Enabled: true})
}

// setCloudfrontDistributionEnabled flips Enabled with a read-modify-write on the current
// config. It retries once when the ETag went stale between the read and the update. It does
// not wait for the change to deploy. found is false when the distribution does not exist.
func setCloudfrontDistributionEnabled(ctx context.Context, client CloudfrontDistributionAPI, id *string, enabled bool) (found bool, err error) {
	for attempt := 0; ; attempt++ {
		out, err := client.GetDistributionConfig(ctx, &cloudfront.GetDistributionConfigInput{Id: id})
		if err != nil {
			if hasErrorCode(err, "NoSuchDistribution") {
				return false, nil
			}
			return true, errors.WithStackTrace(err)
		}
		dc := out.DistributionConfig
		if dc == nil {
			return true, fmt.Errorf("distribution %s has no config", aws.ToString(id))
		}
		if aws.ToBool(dc.Enabled) == enabled {
			return true, nil
		}
		dc.Enabled = aws.Bool(enabled)
		_, err = client.UpdateDistribution(ctx, &cloudfront.UpdateDistributionInput{
			Id:                 id,
			IfMatch:            out.ETag,
			DistributionConfig: dc,
		})
		switch {
		case err == nil:
			return true, nil
		case hasErrorCode(err, "NoSuchDistribution"):
			return false, nil
		case hasErrorCode(err, "PreconditionFailed") && attempt == 0:
			continue
		default:
			return true, errors.WithStackTrace(err)
		}
	}
}

func stopCloudfrontDistribution(ctx context.Context, client CloudfrontDistributionAPI, id *string, _ json.RawMessage) error {
	// A distribution that disappeared has nothing left to stop.
	_, err := setCloudfrontDistributionEnabled(ctx, client, id, false)
	return err
}

func startCloudfrontDistribution(ctx context.Context, client CloudfrontDistributionAPI, id *string, prev json.RawMessage) error {
	var p cloudfrontPrevState
	if err := unmarshalPrev(prev, &p); err != nil {
		return err
	}
	if !p.Enabled {
		return nil
	}
	found, err := setCloudfrontDistributionEnabled(ctx, client, id, true)
	if err != nil {
		return err
	}
	if !found {
		return resource.ErrGone
	}
	return nil
}
