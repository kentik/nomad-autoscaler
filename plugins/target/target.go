// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package target

import (
	"errors"

	"github.com/hashicorp/nomad-autoscaler/plugins/base"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// scalingNoOpCode carries sdk.TargetScalingNoOpError across the plugin gRPC
// boundary, which flattens concrete error types to a status code and message.
// No other error in the Scale path maps to this code.
const scalingNoOpCode = codes.FailedPrecondition

// scaleErrorToStatus encodes an error returned by a Target implementation so
// the client can reconstruct its type.
func scaleErrorToStatus(err error) error {
	var noOpErr *sdk.TargetScalingNoOpError
	if errors.As(err, &noOpErr) {
		return status.Error(scalingNoOpCode, err.Error())
	}

	return err
}

// scaleErrorFromStatus reverses scaleErrorToStatus.
func scaleErrorFromStatus(err error) error {
	if err == nil {
		return nil
	}

	if status.Code(err) == scalingNoOpCode {
		return &sdk.TargetScalingNoOpError{Err: errors.New(status.Convert(err).Message())}
	}

	return err
}

// Target is the interface that all Target plugins are required to implement.
// The plugins are responsible for providing status details of the remote
// target, as well as carrying out scaling actions as decided by the Strategy
// plugin and internal autoscaler controls.
type Target interface {

	// Embed base.Base ensuring that strategy plugins implement this interface.
	base.Base

	// Scale triggers a scaling action against the remote target as specified
	// by the config func argument.
	Scale(action sdk.ScalingAction, config map[string]string) error

	// Status collects and returns critical information of the status of the
	// remote target. The information is used to understand whether the target
	// is in a position to be scaled as well as the current running count which
	// will be used when performing the strategy calculation.
	Status(config map[string]string) (*sdk.TargetStatus, error)
}
