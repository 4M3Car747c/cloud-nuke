package resources

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/aws/smithy-go"
	"github.com/gruntwork-io/cloud-nuke/config"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/gruntwork-io/cloud-nuke/util"
	"github.com/gruntwork-io/go-commons/errors"
)

// rdsWaitTimeout bounds waits for an RDS instance or cluster to leave a transitional state.
const rdsWaitTimeout = 30 * time.Minute

// rdsPollInterval is how often a wait re-reads the status. Tests shorten it.
var rdsPollInterval = 15 * time.Second

// rdsStatusAvailable is the only status recorded in prev: Start brings the resource back to it.
const rdsStatusAvailable = "available"

// rdsPrevState is what Start needs to restore an RDS instance or cluster.
type rdsPrevState struct {
	Status string `json:"status"`
}

// Instance statuses (https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/accessing-monitoring.html).
var (
	// rdsInstanceRunning: billing and can be stopped now. storage-optimization can last
	// for hours after a storage change while the instance is fully usable, so it is not waited out.
	rdsInstanceRunning = []string{"available", "storage-optimization"}
	// rdsInstanceDown: not running, or on its way out; nothing to stop and nothing to restore.
	rdsInstanceDown = []string{"stopped", "stopping", "deleting"}
	// rdsInstanceTransitional: short-lived states that end in available (or stopped/deleting).
	rdsInstanceTransitional = []string{
		"creating", "modifying", "backing-up", "starting", "rebooting", "upgrading",
		"configuring-enhanced-monitoring", "configuring-iam-database-auth", "configuring-log-exports",
		"converting-to-vpc", "moving-to-vpc", "renaming", "resetting-master-credentials",
		"maintenance", "storage-config-upgrade", "delete-precheck",
	}
	// Anything else (failed, storage-full, incompatible-*, inaccessible-encryption-credentials*,
	// insufficient-capacity, restore-error, ...) is broken and reported as an error.
)

func dbInstanceStopHooks() *resource.StopHooks[DBInstancesAPI] {
	return &resource.StopHooks[DBInstancesAPI]{
		List:    listStoppableDBInstances,
		Capture: captureDBInstance,
		Stop:    stopDBInstance,
		Start:   startDBInstance,
	}
}

// listStoppableDBInstances lists standalone instances. Cluster members (Aurora, Multi-AZ DB
// clusters, DocumentDB, Neptune) are stopped through their cluster by the rds-cluster hooks;
// StopDBInstance rejects them. Read replicas are kept so Capture can report them as unsupported.
func listStoppableDBInstances(ctx context.Context, client DBInstancesAPI, _ resource.Scope, cfg config.ResourceType) ([]*string, error) {
	var ids []*string
	paginator := rds.NewDescribeDBInstancesPaginator(client, &rds.DescribeDBInstancesInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, errors.WithStackTrace(err)
		}
		for _, db := range page.DBInstances {
			if db.DBClusterIdentifier != nil {
				continue
			}
			if cfg.ShouldInclude(config.ResourceValue{
				Time: db.InstanceCreateTime,
				Name: db.DBInstanceIdentifier,
				Tags: util.ConvertRDSTypeTagsToMap(db.TagList),
			}) {
				ids = append(ids, db.DBInstanceIdentifier)
			}
		}
	}
	return ids, nil
}

// describeDBInstanceForStop returns the instance, or nil if it does not exist.
func describeDBInstanceForStop(ctx context.Context, client DBInstancesAPI, id *string) (*types.DBInstance, error) {
	out, err := client.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{DBInstanceIdentifier: id})
	if err != nil {
		if hasErrorCode(err, "DBInstanceNotFound", "DBInstanceNotFoundFault") {
			return nil, nil
		}
		return nil, errors.WithStackTrace(err)
	}
	for i := range out.DBInstances {
		if aws.ToString(out.DBInstances[i].DBInstanceIdentifier) == aws.ToString(id) {
			return &out.DBInstances[i], nil
		}
	}
	return nil, nil
}

func dbInstanceStatus(inst *types.DBInstance) string {
	if inst == nil {
		return ""
	}
	return aws.ToString(inst.DBInstanceStatus)
}

// dbInstanceUnsupported returns why AWS cannot stop inst, or "".
func dbInstanceUnsupported(inst *types.DBInstance) string {
	switch {
	case inst.DBClusterIdentifier != nil:
		return fmt.Sprintf("member of DB cluster %s; it is stopped with the cluster", aws.ToString(inst.DBClusterIdentifier))
	case inst.ReadReplicaSourceDBInstanceIdentifier != nil:
		return fmt.Sprintf("read replica of %s", aws.ToString(inst.ReadReplicaSourceDBInstanceIdentifier))
	case inst.ReadReplicaSourceDBClusterIdentifier != nil:
		return fmt.Sprintf("read replica of cluster %s", aws.ToString(inst.ReadReplicaSourceDBClusterIdentifier))
	case len(inst.ReadReplicaDBInstanceIdentifiers) > 0 || len(inst.ReadReplicaDBClusterIdentifiers) > 0:
		return fmt.Sprintf("has read replicas %v", append(slices.Clone(inst.ReadReplicaDBInstanceIdentifiers), inst.ReadReplicaDBClusterIdentifiers...))
	case strings.HasPrefix(aws.ToString(inst.Engine), "sqlserver") && aws.ToBool(inst.MultiAZ):
		return "RDS for SQL Server in a Multi-AZ deployment"
	}
	return ""
}

func captureDBInstance(ctx context.Context, client DBInstancesAPI, id *string) (json.RawMessage, error) {
	inst, err := describeDBInstanceForStop(ctx, client, id)
	if err != nil {
		return nil, err
	}
	status := dbInstanceStatus(inst)
	if inst == nil || slices.Contains(rdsInstanceDown, status) {
		return nil, resource.ErrAlreadyStopped
	}
	// Checked before waiting: no point waiting for something that cannot be stopped anyway.
	if reason := dbInstanceUnsupported(inst); reason != "" {
		return nil, resource.Unsupported("%s", reason)
	}
	if slices.Contains(rdsInstanceTransitional, status) {
		inst, err = rdsWait(ctx, rdsWaitTimeout, func(ctx context.Context) (*types.DBInstance, error) {
			return describeDBInstanceForStop(ctx, client, id)
		}, func(inst *types.DBInstance) bool {
			return !slices.Contains(rdsInstanceTransitional, dbInstanceStatus(inst))
		})
		if err != nil {
			return nil, fmt.Errorf("waiting for DB instance in state %s to settle: %w", status, err)
		}
		status = dbInstanceStatus(inst)
		if inst == nil || slices.Contains(rdsInstanceDown, status) {
			return nil, resource.ErrAlreadyStopped
		}
	}
	if !slices.Contains(rdsInstanceRunning, status) {
		return nil, fmt.Errorf("DB instance is in state %s and cannot be stopped", status)
	}
	return marshalPrev(rdsPrevState{Status: rdsStatusAvailable})
}

// stopDBInstance stops the instance without a snapshot. It does not wait for it to be stopped.
func stopDBInstance(ctx context.Context, client DBInstancesAPI, id *string, _ json.RawMessage) error {
	inst, err := describeDBInstanceForStop(ctx, client, id)
	if err != nil {
		return err
	}
	if inst == nil || slices.Contains(rdsInstanceDown, dbInstanceStatus(inst)) {
		// Gone (Start will report it) or already on its way down.
		return nil
	}
	_, err = client.StopDBInstance(ctx, &rds.StopDBInstanceInput{DBInstanceIdentifier: id})
	if err == nil {
		return nil
	}
	if hasErrorCode(err, "DBInstanceNotFound", "DBInstanceNotFoundFault") {
		return nil
	}
	if hasErrorCode(err, "InvalidDBInstanceState", "InvalidDBInstanceStateFault") {
		// Lost a race with another stop, or the instance is in a state that refuses it.
		if inst, derr := describeDBInstanceForStop(ctx, client, id); derr == nil &&
			(inst == nil || slices.Contains(rdsInstanceDown, dbInstanceStatus(inst))) {
			return nil
		}
	}
	if rdsStopRejected(err) {
		return resource.Unsupported("%v", err)
	}
	return errors.WithStackTrace(err)
}

// startDBInstance starts a stopped instance. It does not wait for it to become available:
// nothing downstream depends on it, and Capture waits out "starting" if it runs again.
func startDBInstance(ctx context.Context, client DBInstancesAPI, id *string, prev json.RawMessage) error {
	var p rdsPrevState
	if err := unmarshalPrev(prev, &p); err != nil {
		return err
	}
	if p.Status != rdsStatusAvailable {
		return nil
	}
	inst, err := describeDBInstanceForStop(ctx, client, id)
	if err != nil {
		return err
	}
	switch dbInstanceStatus(inst) {
	case "", "deleting":
		return resource.ErrGone
	case "stopped":
	case "stopping":
		inst, err = rdsWait(ctx, rdsWaitTimeout, func(ctx context.Context) (*types.DBInstance, error) {
			return describeDBInstanceForStop(ctx, client, id)
		}, func(inst *types.DBInstance) bool {
			return dbInstanceStatus(inst) != "stopping"
		})
		if err != nil {
			return fmt.Errorf("waiting for DB instance to finish stopping: %w", err)
		}
		switch dbInstanceStatus(inst) {
		case "", "deleting":
			return resource.ErrGone
		case "stopped":
		default:
			return nil
		}
	default:
		// available, starting, or anything else that is not stopped: nothing to start.
		return nil
	}

	if _, err := client.StartDBInstance(ctx, &rds.StartDBInstanceInput{DBInstanceIdentifier: id}); err != nil {
		if hasErrorCode(err, "DBInstanceNotFound", "DBInstanceNotFoundFault") {
			return resource.ErrGone
		}
		if hasErrorCode(err, "InvalidDBInstanceState", "InvalidDBInstanceStateFault") {
			// Someone else may have started it meanwhile.
			if inst, derr := describeDBInstanceForStop(ctx, client, id); derr == nil && inst != nil {
				if s := dbInstanceStatus(inst); s != "stopped" && s != "stopping" {
					return nil
				}
			}
		}
		return errors.WithStackTrace(err)
	}
	return nil
}

// rdsStopRejected reports whether a StopDBInstance/StopDBCluster error means the resource can
// never be stopped as configured, rather than "not right now" (for example a member instance
// that is still being modified). The message is matched only on phrases that cannot occur in
// an identifier, and the AWS error message itself is used so identifiers in it are harmless.
func rdsStopRejected(err error) bool {
	if hasErrorCode(err, "InvalidParameterCombination") {
		return true
	}
	if !hasErrorCode(err, "InvalidDBInstanceState", "InvalidDBInstanceStateFault", "InvalidDBClusterStateFault", "InvalidDBClusterState") {
		return false
	}
	var ae smithy.APIError
	if !stderrors.As(err, &ae) {
		return false
	}
	msg := strings.ToLower(ae.ErrorMessage())
	for _, s := range []string{
		"not supported", "is unsupported", "read replica", "read-replica", "cross-region replica", "global cluster",
		"global database", "blue/green", "cannot be stopped", "can't be stopped",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// rdsWait polls read every rdsPollInterval until done reports true, timeout passes or ctx ends.
func rdsWait[T any](ctx context.Context, timeout time.Duration, read func(context.Context) (T, error), done func(T) bool) (T, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		v, err := read(ctx)
		if err != nil {
			return v, err
		}
		if done(v) {
			return v, nil
		}
		select {
		case <-ctx.Done():
			return v, fmt.Errorf("gave up after %s: %w", timeout, ctx.Err())
		case <-time.After(rdsPollInterval):
		}
	}
}
