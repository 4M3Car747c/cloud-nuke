package resources

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/smithy-go"
)

// Shared helpers for StopHooks implementations.

// marshalPrev encodes the state a Start hook needs.
func marshalPrev(v any) (json.RawMessage, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encoding saved state: %w", err)
	}
	return b, nil
}

// unmarshalPrev decodes state saved by marshalPrev.
func unmarshalPrev(prev json.RawMessage, v any) error {
	if err := json.Unmarshal(prev, v); err != nil {
		return fmt.Errorf("decoding saved state: %w", err)
	}
	return nil
}

// apiErrorCode returns the AWS error code of err, or "" if it is not an API error.
func apiErrorCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

// hasErrorCode reports whether err is an AWS API error with one of codes.
func hasErrorCode(err error, codes ...string) bool {
	code := apiErrorCode(err)
	if code == "" {
		return false
	}
	for _, c := range codes {
		if strings.EqualFold(code, c) {
			return true
		}
	}
	return false
}
