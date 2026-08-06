// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package target

import (
	"errors"
	"testing"

	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func Test_scaleErrorRoundTrip(t *testing.T) {
	testCases := []struct {
		name     string
		input    error
		assertFn func(t *testing.T, got error)
	}{
		{
			name:  "no error",
			input: nil,
			assertFn: func(t *testing.T, got error) {
				assert.NoError(t, got)
			},
		},
		{
			name:  "no-op error survives the boundary",
			input: sdk.NewTargetScalingNoOpError("skipping scaling group %s/%s due to active deployment", "envoy-frontend", "envoy-frontend"),
			assertFn: func(t *testing.T, got error) {
				var noOpErr *sdk.TargetScalingNoOpError
				require.ErrorAs(t, got, &noOpErr)
				assert.Equal(t, "skipping scaling group envoy-frontend/envoy-frontend due to active deployment", got.Error())
			},
		},
		{
			name:  "genuine failure is not mistaken for a no-op",
			input: errors.New("failed to scale group a/b: connection refused"),
			assertFn: func(t *testing.T, got error) {
				var noOpErr *sdk.TargetScalingNoOpError
				require.Error(t, got)
				assert.NotErrorIs(t, got, noOpErr)
				assert.False(t, errors.As(got, &noOpErr))
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// The gRPC layer only preserves the status code and message, so
			// round-tripping through it is what the plugin boundary really does.
			encoded := scaleErrorToStatus(tc.input)

			var overTheWire error
			if encoded != nil {
				s, _ := status.FromError(encoded)
				overTheWire = s.Err()
			}

			tc.assertFn(t, scaleErrorFromStatus(overTheWire))
		})
	}
}

func Test_scaleErrorFromStatus_IgnoresOtherCodes(t *testing.T) {
	err := scaleErrorFromStatus(status.Error(codes.Unavailable, "plugin exited"))

	var noOpErr *sdk.TargetScalingNoOpError
	assert.False(t, errors.As(err, &noOpErr))
	assert.Equal(t, codes.Unavailable, status.Code(err))
}
