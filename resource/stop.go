package resource

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/gruntwork-io/cloud-nuke/config"
	"github.com/gruntwork-io/cloud-nuke/util"
)

// ErrAlreadyStopped is returned by StopHooks.Capture when the resource is not running, so there
// is nothing to stop and nothing to restore later.
var ErrAlreadyStopped = errors.New("already stopped")

// ErrGone is returned by StopHooks.Start when the resource no longer exists.
var ErrGone = errors.New("resource no longer exists")

// UnsupportedError means the resource is billable but cannot be stopped (for example an EC2
// instance with an instance-store root volume). Callers should report it rather than retry.
type UnsupportedError struct {
	Reason string
}

func (e *UnsupportedError) Error() string { return "cannot be stopped: " + e.Reason }

// Unsupported builds an *UnsupportedError.
func Unsupported(format string, args ...any) error {
	return &UnsupportedError{Reason: fmt.Sprintf(format, args...)}
}

// IsUnsupported reports whether err is (or wraps) an *UnsupportedError.
func IsUnsupported(err error) bool {
	var u *UnsupportedError
	return errors.As(err, &u)
}

// StopHooks lets a resource type be stopped and started again instead of deleted.
//
// Stopping is split into Capture and Stop so a caller can persist the captured state before
// anything changes. If the process dies between the two, a rerun finds the resource still
// running (or already stopped) and the saved state is still there to restore from.
//
// All hooks must be idempotent: Stop and Start may be called again for a resource that is
// already in (or moving to) the target state.
type StopHooks[C any] struct {
	// List returns the IDs of resources that may need stopping. Nil means Resource.Lister.
	// Implement it when the nuke lister filters out things that must still be stopped
	// (e.g. termination-protected EC2 instances) or includes things that cannot be.
	List func(ctx context.Context, client C, scope Scope, cfg config.ResourceType) ([]*string, error)

	// Capture returns the state needed to restore the resource. It returns ErrAlreadyStopped
	// when there is nothing to stop and an *UnsupportedError when the resource cannot be stopped.
	Capture func(ctx context.Context, client C, id *string) (json.RawMessage, error)

	// Stop stops the resource. prev is what Capture returned.
	Stop func(ctx context.Context, client C, id *string, prev json.RawMessage) error

	// Start restores the resource to prev. It returns ErrGone when the resource no longer exists.
	Start func(ctx context.Context, client C, id *string, prev json.RawMessage) error
}

// StoppableResource is implemented by every Resource[C]. Stoppable reports whether the
// resource type actually has stop hooks.
type StoppableResource interface {
	ResourceName() string
	Stoppable() bool
	ListStoppable(ctx context.Context, configObj config.Config) ([]string, error)
	CaptureState(ctx context.Context, id string) (json.RawMessage, error)
	StopResource(ctx context.Context, id string, prev json.RawMessage) error
	StartResource(ctx context.Context, id string, prev json.RawMessage) error
}

// Stoppable reports whether this resource type supports stop and start.
func (r *Resource[C]) Stoppable() bool {
	return r.Stop != nil && r.Stop.Capture != nil && r.Stop.Stop != nil && r.Stop.Start != nil
}

func (r *Resource[C]) checkStoppable() error {
	if !r.Stoppable() {
		return fmt.Errorf("%s: stop hooks not configured", r.ResourceTypeName)
	}
	return r.InitializationError
}

// ListStoppable lists the resources that may need stopping, applying the resource's config filters.
func (r *Resource[C]) ListStoppable(ctx context.Context, configObj config.Config) ([]string, error) {
	if err := r.checkStoppable(); err != nil {
		return nil, err
	}
	var cfg config.ResourceType
	if r.ConfigGetter != nil {
		cfg = r.ConfigGetter(configObj)
	}
	list := r.Stop.List
	if list == nil {
		list = r.Lister
	}
	if list == nil {
		return nil, fmt.Errorf("%s: no lister configured", r.ResourceTypeName)
	}
	ids, err := list(ctx, r.Client, r.Scope, cfg)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to list resources in %s: %w", r.ResourceTypeName, r.Scope, err)
	}
	return util.DerefStringSlice(ids), nil
}

// CaptureState runs StopHooks.Capture for one resource.
func (r *Resource[C]) CaptureState(ctx context.Context, id string) (json.RawMessage, error) {
	if err := r.checkStoppable(); err != nil {
		return nil, err
	}
	return r.Stop.Capture(ctx, r.Client, &id)
}

// StopResource runs StopHooks.Stop for one resource.
func (r *Resource[C]) StopResource(ctx context.Context, id string, prev json.RawMessage) error {
	if err := r.checkStoppable(); err != nil {
		return err
	}
	return r.Stop.Stop(ctx, r.Client, &id, prev)
}

// StartResource runs StopHooks.Start for one resource.
func (r *Resource[C]) StartResource(ctx context.Context, id string, prev json.RawMessage) error {
	if err := r.checkStoppable(); err != nil {
		return err
	}
	return r.Stop.Start(ctx, r.Client, &id, prev)
}
