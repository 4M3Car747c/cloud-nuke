package resources

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/gruntwork-io/go-commons/errors"
)

// lambdaPrevState is what Start needs to restore a function.
//
// Reserved is the function's reserved concurrency before the stop; nil means it was unset
// (the function used the unreserved pool). Provisioned maps each qualifier (alias or
// version) that had provisioned concurrency to the requested amount. Provisioned
// concurrency bills around the clock whether or not the function runs, and Lambda rejects
// a reserved concurrency below the provisioned total, so Stop removes it first.
type lambdaPrevState struct {
	Reserved    *int32           `json:"reserved"`
	Provisioned map[string]int32 `json:"provisioned,omitempty"`
}

func lambdaStopHooks() *resource.StopHooks[LambdaFunctionsAPI] {
	return &resource.StopHooks[LambdaFunctionsAPI]{
		// The nuke lister lists every function with its tags, which is what we want.
		Capture: captureLambdaFunction,
		Stop:    stopLambdaFunction,
		Start:   startLambdaFunction,
	}
}

// lambdaProvisioned returns the requested provisioned concurrency per qualifier.
func lambdaProvisioned(ctx context.Context, client LambdaFunctionsAPI, name *string) (map[string]int32, error) {
	out := map[string]int32{}
	paginator := lambda.NewListProvisionedConcurrencyConfigsPaginator(client, &lambda.ListProvisionedConcurrencyConfigsInput{FunctionName: name})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, c := range page.ProvisionedConcurrencyConfigs {
			// FunctionArn is arn:aws:lambda:region:account:function:name:qualifier.
			arn := aws.ToString(c.FunctionArn)
			q := arn[strings.LastIndex(arn, ":")+1:]
			if n := aws.ToInt32(c.RequestedProvisionedConcurrentExecutions); n > 0 && q != "" {
				out[q] = n
			}
		}
	}
	return out, nil
}

func captureLambdaFunction(ctx context.Context, client LambdaFunctionsAPI, name *string) (json.RawMessage, error) {
	conc, err := client.GetFunctionConcurrency(ctx, &lambda.GetFunctionConcurrencyInput{FunctionName: name})
	if err != nil {
		if hasErrorCode(err, "ResourceNotFoundException") {
			return nil, resource.ErrAlreadyStopped
		}
		return nil, errors.WithStackTrace(err)
	}
	if conc.ReservedConcurrentExecutions != nil && *conc.ReservedConcurrentExecutions == 0 {
		return nil, resource.ErrAlreadyStopped
	}
	prov, err := lambdaProvisioned(ctx, client, name)
	if err != nil {
		if hasErrorCode(err, "ResourceNotFoundException") {
			return nil, resource.ErrAlreadyStopped
		}
		return nil, errors.WithStackTrace(err)
	}
	return marshalPrev(lambdaPrevState{Reserved: conc.ReservedConcurrentExecutions, Provisioned: prov})
}

// stopLambdaFunction removes provisioned concurrency (re-read, not taken from prev, so a
// rerun cleans up whatever is there) and then sets reserved concurrency to 0, which throttles
// every invocation.
func stopLambdaFunction(ctx context.Context, client LambdaFunctionsAPI, name *string, _ json.RawMessage) error {
	prov, err := lambdaProvisioned(ctx, client, name)
	if err != nil {
		if hasErrorCode(err, "ResourceNotFoundException") {
			return nil
		}
		return errors.WithStackTrace(err)
	}
	for _, q := range slices.Sorted(maps.Keys(prov)) {
		_, err := client.DeleteProvisionedConcurrencyConfig(ctx, &lambda.DeleteProvisionedConcurrencyConfigInput{
			FunctionName: name,
			Qualifier:    aws.String(q),
		})
		if err != nil && !hasErrorCode(err, "ProvisionedConcurrencyConfigNotFoundException", "ResourceNotFoundException") {
			return errors.WithStackTrace(err)
		}
	}
	_, err = client.PutFunctionConcurrency(ctx, &lambda.PutFunctionConcurrencyInput{
		FunctionName:                 name,
		ReservedConcurrentExecutions: aws.Int32(0),
	})
	if err != nil {
		if hasErrorCode(err, "ResourceNotFoundException") {
			return nil
		}
		return errors.WithStackTrace(err)
	}
	return nil
}

// startLambdaFunction restores reserved concurrency, then provisioned concurrency (which must
// fit inside the reserved amount). It does not wait for provisioned concurrency to be READY.
func startLambdaFunction(ctx context.Context, client LambdaFunctionsAPI, name *string, prev json.RawMessage) error {
	var p lambdaPrevState
	if err := unmarshalPrev(prev, &p); err != nil {
		return err
	}
	var err error
	if p.Reserved == nil {
		_, err = client.DeleteFunctionConcurrency(ctx, &lambda.DeleteFunctionConcurrencyInput{FunctionName: name})
	} else {
		_, err = client.PutFunctionConcurrency(ctx, &lambda.PutFunctionConcurrencyInput{
			FunctionName:                 name,
			ReservedConcurrentExecutions: p.Reserved,
		})
	}
	if err != nil {
		if hasErrorCode(err, "ResourceNotFoundException") {
			return resource.ErrGone
		}
		return errors.WithStackTrace(err)
	}
	for _, q := range slices.Sorted(maps.Keys(p.Provisioned)) {
		_, err := client.PutProvisionedConcurrencyConfig(ctx, &lambda.PutProvisionedConcurrencyConfigInput{
			FunctionName:                    name,
			Qualifier:                       aws.String(q),
			ProvisionedConcurrentExecutions: aws.Int32(p.Provisioned[q]),
		})
		if err != nil {
			// The alias or version was removed while the function was stopped; nothing to
			// put the provisioned concurrency back on.
			if hasErrorCode(err, "ResourceNotFoundException") {
				continue
			}
			return errors.WithStackTrace(err)
		}
	}
	return nil
}
