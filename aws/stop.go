package aws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/sync/errgroup"

	"github.com/gruntwork-io/cloud-nuke/aws/resources"
	"github.com/gruntwork-io/cloud-nuke/config"
	"github.com/gruntwork-io/cloud-nuke/logging"
	"github.com/gruntwork-io/cloud-nuke/reporting"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/gruntwork-io/cloud-nuke/util"
)

// StopOrder is the order resource types are stopped in within a region. Start runs it in
// reverse. Auto Scaling groups come first so they cannot replace instances that are being
// stopped, and they are resumed last, after their instances are running again.
// Global resources (CloudFront) are stopped after every region and started before them.
var StopOrder = []string{
	"asg",
	"eks-cluster",
	"ecs-service",
	"ec2",
	"rds-instance",
	"rds-cluster",
	"elasticache",
	"elasticache-serverless",
	"lambda",
	"cloudfront-distribution",
}

// stopConcurrency is how many resources of one type in one region are handled at once.
const stopConcurrency = 8

// StopStatus is the outcome of stopping or starting one resource.
type StopStatus string

const (
	// StopStatusStopping: state was captured and the stop call is about to be made.
	// Persist Prev when you see this; it is what Start needs.
	StopStatusStopping StopStatus = "stopping"
	// StopStatusStopped: the stop call succeeded.
	StopStatusStopped StopStatus = "stopped"
	// StopStatusSkipped: the resource was already stopped, so there is nothing to restore.
	// If an earlier run recorded it as stopping, it can now be treated as stopped.
	StopStatusSkipped StopStatus = "skipped"
	// StopStatusUnsupported: the resource is running but this type of it cannot be stopped.
	StopStatusUnsupported StopStatus = "unsupported"
	// StopStatusFailed: capture, stop or start failed; Error says why.
	StopStatusFailed StopStatus = "failed"
	// StopStatusRestored: Start succeeded.
	StopStatusRestored StopStatus = "restored"
	// StopStatusGone: the resource no longer exists, so it could not be started.
	StopStatusGone StopStatus = "gone"
)

// StopRecord describes one resource that StopAllResources handled. Records with Status
// stopped (or stopping, after a crash) are what StartResources takes to restore them.
type StopRecord struct {
	ResourceType string          `json:"resource_type"`
	Region       string          `json:"region"`
	ID           string          `json:"id"`
	Prev         json.RawMessage `json:"prev,omitempty"`
	Status       StopStatus      `json:"status"`
	Error        string          `json:"error,omitempty"`
}

// RecordFunc receives every record as soon as it is produced. For a stopping record,
// returning an error skips the stop call, so a resource is never stopped without its
// state having been saved. Errors for other statuses are logged and otherwise ignored.
// It may be called from several goroutines at once.
//
// When a resource already has a saved Prev from an earlier run, keep that one: a run that
// crashed half-way through a multi-step stop (e.g. Lambda provisioned concurrency removed,
// reserved concurrency not yet 0) captures a partly stopped state on the rerun.
type RecordFunc func(StopRecord) error

// ResourceStopEvent is emitted for each record StopAllResources or StartResources produces.
type ResourceStopEvent struct {
	StopRecord
}

func (ResourceStopEvent) EventType() string { return "resource_stop" }

// StoppableResourceTypes returns the resource types that support stop and start, in StopOrder.
func StoppableResourceTypes() []string {
	var names []string
	for _, r := range stoppableResources(GlobalRegion, nil) {
		names = append(names, r.ResourceName())
	}
	for _, r := range stoppableResources("", nil) {
		names = append(names, r.ResourceName())
	}
	return names
}

// stoppableResources returns uninitialized stoppable resources for region ("global" or any
// regional name), filtered by types (empty = all) and sorted by StopOrder.
func stoppableResources(region string, types []string) []resource.StoppableResource {
	var registered []resources.AwsResource
	if region == GlobalRegion {
		registered = getRegisteredGlobalResources()
	} else {
		registered = getRegisteredRegionalResources()
	}
	var out []resource.StoppableResource
	for _, r := range registered {
		s, ok := r.(resource.StoppableResource)
		if !ok || !s.Stoppable() || !IsNukeable(r.ResourceName(), types) {
			continue
		}
		out = append(out, s)
	}
	slices.SortStableFunc(out, func(a, b resource.StoppableResource) int {
		return stopRank(a.ResourceName()) - stopRank(b.ResourceName())
	})
	return out
}

// stopRank orders types by StopOrder; types missing from it go last.
func stopRank(name string) int {
	if i := slices.Index(StopOrder, name); i >= 0 {
		return i
	}
	return len(StopOrder)
}

// stoppableFactory returns the initialized stoppable resources for one region, in StopOrder.
// Tests replace it with fakes.
type stoppableFactory func(query *Query, region string, types []string) ([]resource.StoppableResource, error)

// initStoppable initializes resources with a session for region.
func initStoppable(query *Query, region string, types []string) ([]resource.StoppableResource, error) {
	session, err := query.session(region)
	if err != nil {
		return nil, err
	}
	res := stoppableResources(region, types)
	for _, r := range res {
		r.(resources.AwsResource).Init(session)
	}
	return res, nil
}

// StopAllResources stops every stoppable resource in query.Regions (include GlobalRegion for
// CloudFront) instead of deleting it. Within a region, types are handled one after another in
// StopOrder; regions run in parallel (query.Parallelism), and the global region runs last.
//
// Only query.Regions, ResourceTypes, IncludeTags, IgnoreProtectionTags, Parallelism and
// ConfigProvider are used.
// configObj filters which resources are included, like for GetAllResources.
//
// The returned error covers listing failures only; per-resource failures are in the records.
func StopAllResources(ctx context.Context, query *Query, configObj config.Config, onRecord RecordFunc, collector *reporting.Collector) ([]StopRecord, error) {
	return stopAll(ctx, query, configObj, onRecord, collector, initStoppable)
}

func stopAll(ctx context.Context, query *Query, configObj config.Config, onRecord RecordFunc, collector *reporting.Collector, factory stoppableFactory) ([]StopRecord, error) {
	configObj.AddIncludeTags(query.IncludeTags)
	if query.IgnoreProtectionTags {
		configObj.IgnoreProtectionTags()
	}
	run := newStopRun(onRecord, collector)

	stopRegion := func(region string) error {
		res, err := factory(query, region, query.ResourceTypes)
		if err != nil {
			return fmt.Errorf("[%s] %w", region, err)
		}
		var errs *multierror.Error
		for _, r := range res {
			ids, err := r.ListStoppable(ctx, configObj)
			if err != nil {
				// Keep going: stopping the rest is better than stopping nothing.
				logging.Errorf("Unable to list %s in %s for stopping: %v", r.ResourceName(), region, err)
				emit(collector, reporting.GeneralError{
					ResourceType: r.ResourceName(),
					Description:  fmt.Sprintf("Unable to list %s in %s", r.ResourceName(), region),
					Error:        err.Error(),
				})
				errs = multierror.Append(errs, fmt.Errorf("[%s] %s: %w", region, r.ResourceName(), err))
				continue
			}
			forEach(ctx, ids, func(id string) {
				run.stopOne(ctx, r, region, id)
			})
		}
		return errs.ErrorOrNil()
	}

	err := eachRegion(query, stopRegion, false)
	return run.records, err
}

// StartResources restores the resources in records, which come from StopAllResources (any
// status; records without Prev are reported as failed). It runs StopOrder in reverse within
// each region; the global region goes first, then regions in parallel (query.Parallelism).
//
// Only query.Parallelism and query.ConfigProvider are used. The returned error covers session
// failures only; per-resource outcomes are in the records (restored, gone or failed).
func StartResources(ctx context.Context, query *Query, records []StopRecord, onRecord RecordFunc, collector *reporting.Collector) ([]StopRecord, error) {
	return startAll(ctx, query, records, onRecord, collector, initStoppable)
}

func startAll(ctx context.Context, query *Query, records []StopRecord, onRecord RecordFunc, collector *reporting.Collector, factory stoppableFactory) ([]StopRecord, error) {
	run := newStopRun(onRecord, collector)

	byRegion := map[string][]StopRecord{}
	var regions []string
	for _, rec := range records {
		if _, ok := byRegion[rec.Region]; !ok {
			regions = append(regions, rec.Region)
		}
		byRegion[rec.Region] = append(byRegion[rec.Region], rec)
	}

	startRegion := func(region string) error {
		recs := byRegion[region]
		res, err := factory(query, region, nil)
		if err != nil {
			for _, rec := range recs {
				run.finish(rec, StopStatusFailed, err)
			}
			return fmt.Errorf("[%s] %w", region, err)
		}
		known := map[string]bool{}
		for i := len(res) - 1; i >= 0; i-- {
			r := res[i]
			known[r.ResourceName()] = true
			var todo []StopRecord
			for _, rec := range recs {
				if rec.ResourceType == r.ResourceName() {
					todo = append(todo, rec)
				}
			}
			forEach(ctx, todo, func(rec StopRecord) {
				run.startOne(ctx, r, rec)
			})
		}
		for _, rec := range recs {
			if !known[rec.ResourceType] {
				run.finish(rec, StopStatusFailed, fmt.Errorf("resource type %q cannot be started in %s", rec.ResourceType, region))
			}
		}
		return nil
	}

	q := *query
	q.Regions = regions
	err := eachRegion(&q, startRegion, true)
	return run.records, err
}

// eachRegion runs fn for every region in query.Regions, regional ones in parallel. The global
// region runs after them, or before them when globalFirst is set.
func eachRegion(query *Query, fn func(region string) error, globalFirst bool) error {
	var mu sync.Mutex
	var errs *multierror.Error
	call := func(region string) {
		if err := fn(region); err != nil {
			mu.Lock()
			errs = multierror.Append(errs, err)
			mu.Unlock()
		}
	}
	hasGlobal := slices.Contains(query.Regions, GlobalRegion)
	if hasGlobal && globalFirst {
		call(GlobalRegion)
	}
	p := query.Parallelism
	if p <= 0 {
		p = util.DefaultParallelism
	}
	eg := new(errgroup.Group)
	eg.SetLimit(p)
	for _, region := range query.Regions {
		if region == GlobalRegion {
			continue
		}
		eg.Go(func() error {
			call(region)
			return nil
		})
	}
	_ = eg.Wait()
	if hasGlobal && !globalFirst {
		call(GlobalRegion)
	}
	return errs.ErrorOrNil()
}

// forEach runs fn for each item with bounded concurrency and waits for all of them.
func forEach[T any](ctx context.Context, items []T, fn func(T)) {
	eg, _ := errgroup.WithContext(ctx)
	eg.SetLimit(stopConcurrency)
	for _, it := range items {
		eg.Go(func() error {
			fn(it)
			return nil
		})
	}
	_ = eg.Wait()
}

// stopRun collects records and forwards them to the callback and collector.
type stopRun struct {
	mu        sync.Mutex
	records   []StopRecord
	onRecord  RecordFunc
	collector *reporting.Collector
}

func newStopRun(onRecord RecordFunc, collector *reporting.Collector) *stopRun {
	return &stopRun{onRecord: onRecord, collector: collector}
}

// report forwards rec to the callback and collector. Terminal records are also kept.
func (s *stopRun) report(rec StopRecord) error {
	var err error
	if s.onRecord != nil {
		err = s.onRecord(rec)
	}
	emit(s.collector, ResourceStopEvent{StopRecord: rec})
	if rec.Status != StopStatusStopping {
		s.mu.Lock()
		s.records = append(s.records, rec)
		s.mu.Unlock()
	}
	return err
}

// finish reports rec with a final status, setting Error from err.
func (s *stopRun) finish(rec StopRecord, status StopStatus, err error) {
	rec.Status = status
	rec.Error = ""
	if err != nil {
		rec.Error = err.Error()
	}
	switch status {
	case StopStatusFailed:
		logging.Errorf("[Failed] %s %s %s: %s", rec.ResourceType, rec.Region, rec.ID, rec.Error)
	case StopStatusUnsupported:
		logging.Warnf("[Unsupported] %s %s %s: %s", rec.ResourceType, rec.Region, rec.ID, rec.Error)
	default:
		logging.Infof("[%s] %s %s %s", status, rec.ResourceType, rec.Region, rec.ID)
	}
	if err := s.report(rec); err != nil {
		logging.Errorf("Recording %s %s %s as %s failed: %v", rec.ResourceType, rec.Region, rec.ID, status, err)
	}
}

func (s *stopRun) stopOne(ctx context.Context, r resource.StoppableResource, region, id string) {
	rec := StopRecord{ResourceType: r.ResourceName(), Region: region, ID: id}
	prev, err := r.CaptureState(ctx, id)
	switch {
	case errors.Is(err, resource.ErrAlreadyStopped):
		s.finish(rec, StopStatusSkipped, nil)
		return
	case resource.IsUnsupported(err):
		s.finish(rec, StopStatusUnsupported, err)
		return
	case err != nil:
		s.finish(rec, StopStatusFailed, fmt.Errorf("capture: %w", err))
		return
	}

	rec.Prev = prev
	rec.Status = StopStatusStopping
	if err := s.report(rec); err != nil {
		s.finish(rec, StopStatusFailed, fmt.Errorf("saving state before stop: %w", err))
		return
	}

	err = r.StopResource(ctx, id, prev)
	switch {
	case err == nil:
		s.finish(rec, StopStatusStopped, nil)
	case resource.IsUnsupported(err):
		s.finish(rec, StopStatusUnsupported, err)
	default:
		s.finish(rec, StopStatusFailed, fmt.Errorf("stop: %w", err))
	}
}

func (s *stopRun) startOne(ctx context.Context, r resource.StoppableResource, rec StopRecord) {
	if len(rec.Prev) == 0 {
		s.finish(rec, StopStatusFailed, errors.New("no saved state to start from"))
		return
	}
	err := r.StartResource(ctx, rec.ID, rec.Prev)
	switch {
	case err == nil:
		s.finish(rec, StopStatusRestored, nil)
	case errors.Is(err, resource.ErrGone):
		s.finish(rec, StopStatusGone, nil)
	default:
		s.finish(rec, StopStatusFailed, fmt.Errorf("start: %w", err))
	}
}

func emit(c *reporting.Collector, e reporting.Event) {
	if c != nil {
		c.Emit(e)
	}
}
