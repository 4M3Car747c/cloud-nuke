package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/gruntwork-io/go-commons/errors"
)

// Cluster statuses (https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Status.html).
// The same API and statuses cover Aurora, Multi-AZ DB clusters, DocumentDB and Neptune.
var (
	rdsClusterRunning      = []string{"available", "storage-optimization"}
	rdsClusterDown         = []string{"stopped", "stopping", "deleting"}
	rdsClusterTransitional = []string{
		"creating", "modifying", "backing-up", "starting", "renaming", "resetting-master-credentials",
		"upgrading", "maintenance", "failing-over", "backtracking", "promoting", "update-iam-db-auth",
		"migrating", "scaling-compute", "scaling-capacity",
	}
	// Anything else (inaccessible-encryption-credentials*, migration-failed, cloning-failed,
	// failed, ...) is reported as an error.
)

func dbClusterStopHooks() *resource.StopHooks[DBClustersAPI] {
	return &resource.StopHooks[DBClustersAPI]{
		// The nuke lister lists every cluster; Capture sorts out the ones that cannot be stopped.
		List:    listDBClusters,
		Capture: captureDBCluster,
		Stop:    stopDBCluster,
		Start:   startDBCluster,
	}
}

// describeDBClusterForStop returns the cluster, or nil if it does not exist.
func describeDBClusterForStop(ctx context.Context, client DBClustersAPI, id *string) (*types.DBCluster, error) {
	out, err := client.DescribeDBClusters(ctx, &rds.DescribeDBClustersInput{DBClusterIdentifier: id})
	if err != nil {
		if hasErrorCode(err, "DBClusterNotFoundFault", "DBClusterNotFound") {
			return nil, nil
		}
		return nil, errors.WithStackTrace(err)
	}
	for i := range out.DBClusters {
		if aws.ToString(out.DBClusters[i].DBClusterIdentifier) == aws.ToString(id) {
			return &out.DBClusters[i], nil
		}
	}
	return nil, nil
}

func dbClusterStatus(c *types.DBCluster) string {
	if c == nil {
		return ""
	}
	return aws.ToString(c.Status)
}

// dbClusterUnsupported returns why AWS cannot stop c, or "".
// Global database membership (more than one cluster) and blue/green deployments are not
// visible here; StopDBCluster rejects those and stopDBCluster maps that to unsupported.
func dbClusterUnsupported(c *types.DBCluster) string {
	switch {
	case aws.ToString(c.EngineMode) == "serverless":
		return "Aurora Serverless v1 cluster (scales to zero by itself; no stop API)"
	case c.ReplicationSourceIdentifier != nil:
		return fmt.Sprintf("read replica of %s", aws.ToString(c.ReplicationSourceIdentifier))
	case len(c.ReadReplicaIdentifiers) > 0:
		return fmt.Sprintf("has read replicas %v", c.ReadReplicaIdentifiers)
	}
	return ""
}

func captureDBCluster(ctx context.Context, client DBClustersAPI, id *string) (json.RawMessage, error) {
	c, err := describeDBClusterForStop(ctx, client, id)
	if err != nil {
		return nil, err
	}
	status := dbClusterStatus(c)
	if c == nil || slices.Contains(rdsClusterDown, status) {
		return nil, resource.ErrAlreadyStopped
	}
	if reason := dbClusterUnsupported(c); reason != "" {
		return nil, resource.Unsupported("%s", reason)
	}
	if slices.Contains(rdsClusterTransitional, status) {
		c, err = rdsWait(ctx, rdsWaitTimeout, func(ctx context.Context) (*types.DBCluster, error) {
			return describeDBClusterForStop(ctx, client, id)
		}, func(c *types.DBCluster) bool {
			return !slices.Contains(rdsClusterTransitional, dbClusterStatus(c))
		})
		if err != nil {
			return nil, fmt.Errorf("waiting for DB cluster in state %s to settle: %w", status, err)
		}
		status = dbClusterStatus(c)
		if c == nil || slices.Contains(rdsClusterDown, status) {
			return nil, resource.ErrAlreadyStopped
		}
	}
	if !slices.Contains(rdsClusterRunning, status) {
		return nil, fmt.Errorf("DB cluster is in state %s and cannot be stopped", status)
	}
	if err := waitDBClusterMembers(ctx, client, c); err != nil {
		return nil, err
	}
	return marshalPrev(rdsPrevState{Status: rdsStatusAvailable})
}

// waitDBClusterMembers waits until no member instance of c is in a transitional state.
// A cluster reports available while its instances are still being created or modified, and
// StopDBCluster then fails with InvalidDBInstanceState "Instance x is not in available state".
func waitDBClusterMembers(ctx context.Context, client DBClustersAPI, c *types.DBCluster) error {
	if len(c.DBClusterMembers) == 0 {
		return nil
	}
	read := func(ctx context.Context) ([]types.DBInstance, error) {
		out, err := client.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{
			Filters: []types.Filter{{Name: aws.String("db-cluster-id"), Values: []string{aws.ToString(c.DBClusterIdentifier)}}},
		})
		if err != nil {
			return nil, errors.WithStackTrace(err)
		}
		return out.DBInstances, nil
	}
	members, err := rdsWait(ctx, rdsWaitTimeout, read, func(members []types.DBInstance) bool {
		for i := range members {
			if slices.Contains(rdsInstanceTransitional, dbInstanceStatus(&members[i])) {
				return false
			}
		}
		return true
	})
	if err != nil {
		return fmt.Errorf("waiting for DB cluster members to settle: %w", err)
	}
	for i := range members {
		if s := dbInstanceStatus(&members[i]); !slices.Contains(rdsInstanceRunning, s) {
			return fmt.Errorf("DB cluster member %s is in state %s; the cluster cannot be stopped",
				aws.ToString(members[i].DBInstanceIdentifier), s)
		}
	}
	return nil
}

// stopDBCluster stops the cluster and with it all its instances. It does not wait.
func stopDBCluster(ctx context.Context, client DBClustersAPI, id *string, _ json.RawMessage) error {
	c, err := describeDBClusterForStop(ctx, client, id)
	if err != nil {
		return err
	}
	if c == nil || slices.Contains(rdsClusterDown, dbClusterStatus(c)) {
		return nil
	}
	_, err = client.StopDBCluster(ctx, &rds.StopDBClusterInput{DBClusterIdentifier: id})
	if err == nil {
		return nil
	}
	if hasErrorCode(err, "DBClusterNotFoundFault", "DBClusterNotFound") {
		return nil
	}
	if hasErrorCode(err, "InvalidDBClusterStateFault", "InvalidDBClusterState") {
		if c, derr := describeDBClusterForStop(ctx, client, id); derr == nil &&
			(c == nil || slices.Contains(rdsClusterDown, dbClusterStatus(c))) {
			return nil
		}
	}
	if rdsStopRejected(err) {
		return resource.Unsupported("%v", err)
	}
	return errors.WithStackTrace(err)
}

// startDBCluster starts a stopped cluster. It does not wait for it to become available.
func startDBCluster(ctx context.Context, client DBClustersAPI, id *string, prev json.RawMessage) error {
	var p rdsPrevState
	if err := unmarshalPrev(prev, &p); err != nil {
		return err
	}
	if p.Status != rdsStatusAvailable {
		return nil
	}
	c, err := describeDBClusterForStop(ctx, client, id)
	if err != nil {
		return err
	}
	switch dbClusterStatus(c) {
	case "", "deleting":
		return resource.ErrGone
	case "stopped":
	case "stopping":
		c, err = rdsWait(ctx, rdsWaitTimeout, func(ctx context.Context) (*types.DBCluster, error) {
			return describeDBClusterForStop(ctx, client, id)
		}, func(c *types.DBCluster) bool {
			return dbClusterStatus(c) != "stopping"
		})
		if err != nil {
			return fmt.Errorf("waiting for DB cluster to finish stopping: %w", err)
		}
		switch dbClusterStatus(c) {
		case "", "deleting":
			return resource.ErrGone
		case "stopped":
		default:
			return nil
		}
	default:
		return nil
	}

	if _, err := client.StartDBCluster(ctx, &rds.StartDBClusterInput{DBClusterIdentifier: id}); err != nil {
		if hasErrorCode(err, "DBClusterNotFoundFault", "DBClusterNotFound") {
			return resource.ErrGone
		}
		if hasErrorCode(err, "InvalidDBClusterStateFault", "InvalidDBClusterState") {
			if c, derr := describeDBClusterForStop(ctx, client, id); derr == nil && c != nil {
				if s := dbClusterStatus(c); s != "stopped" && s != "stopping" {
					return nil
				}
			}
		}
		return errors.WithStackTrace(err)
	}
	return nil
}
