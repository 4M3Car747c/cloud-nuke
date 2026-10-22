package aws

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/gruntwork-io/cloud-nuke/config"
)

// Query is a struct that represents the desired parameters for scanning resources within a given account
type Query struct {
	Regions              []string
	ExcludeRegions       []string
	ResourceTypes        []string
	ExcludeResourceTypes []string
	ExcludeAfter         *time.Time
	IncludeAfter         *time.Time
	ListUnaliasedKMSKeys bool
	Timeout              *time.Duration
	ExcludeFirstSeen     bool
	DefaultOnly          bool
	IncludeTags          map[string]config.Expression
	Parallelism          int

	// ConfigProvider supplies credentials for this query only. Nil falls back to the
	// process-wide externalcreds provider. Setting it lets several accounts be scanned,
	// stopped or started concurrently in one process.
	ConfigProvider func(region string) (aws.Config, error)

	// IgnoreProtectionTags makes StopAllResources ignore the cloud-nuke-excluded and
	// cloud-nuke-after tags (see config.Config.IgnoreProtectionTags).
	IgnoreProtectionTags bool
}

// session returns the AWS config for region using the query's ConfigProvider, if any.
func (q *Query) session(region string) (aws.Config, error) {
	if q == nil {
		return NewSession(region)
	}
	return newSessionWith(q.ConfigProvider, region)
}

// Validate ensures the configured values for a Query are valid, returning an error if there are
// any invalid params, or nil if the Query is valid
func (q *Query) Validate() error {
	resourceTypes, err := HandleResourceTypeSelections(q.ResourceTypes, q.ExcludeResourceTypes)
	if err != nil {
		return err
	}

	q.ResourceTypes = resourceTypes

	regions, err := getEnabledRegions(q.ConfigProvider)
	if err != nil {
		return CouldNotDetermineEnabledRegionsError{Underlying: err}
	}

	// global is a fake region, used to represent global resources
	regions = append(regions, GlobalRegion)

	targetRegions, err := GetTargetRegions(regions, q.Regions, q.ExcludeRegions)
	if err != nil {
		return CouldNotSelectRegionError{Underlying: err}
	}

	q.Regions = targetRegions

	if q.Parallelism < 0 {
		return fmt.Errorf("--parallelism must be >= 0 (0 uses the default)")
	}

	return nil
}
