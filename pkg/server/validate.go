// Copyright (c) 2024 Sidero Labs, Inc.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

package server

import (
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/siderolabs/discovery-service/pkg/limits"
)

func validateClusterID(id string) error {
	if len(id) < 1 {
		return status.Errorf(codes.InvalidArgument, "cluster id can't be empty")
	}

	if len(id) > limits.ClusterIDMax {
		return status.Errorf(codes.InvalidArgument, "cluster id is too long")
	}

	return nil
}

func validateAffiliateID(id string) error {
	if len(id) < 1 {
		return status.Errorf(codes.InvalidArgument, "affiliate id can't be empty")
	}

	if len(id) > limits.AffiliateIDMax {
		return status.Errorf(codes.InvalidArgument, "affiliate id is too long")
	}

	return nil
}

func validateAffiliateData(data []byte) error {
	if len(data) > limits.AffiliateDataMax {
		return status.Error(codes.InvalidArgument, "affiliate data is too big")
	}

	return nil
}

func validateAffiliateEndpoints(endpoints [][]byte) error {
	for _, endpoint := range endpoints {
		if len(endpoint) > limits.AffiliateEndpointMax {
			return status.Errorf(codes.InvalidArgument, "affiliate endpoint is too big")
		}
	}

	return nil
}

func validateTTL(ttl time.Duration) error {
	if ttl > limits.TTLMax {
		return status.Errorf(codes.InvalidArgument, "ttl is too large")
	}

	return nil
}
