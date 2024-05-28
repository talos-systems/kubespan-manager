// Copyright (c) 2024 Sidero Labs, Inc.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

package storage_test

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	storagepb "github.com/siderolabs/discovery-service/api/storage"
	"github.com/siderolabs/discovery-service/internal/state/storage"
	"github.com/siderolabs/discovery-service/pkg/state"
)

func BenchmarkExport(b *testing.B) {
	logger := zap.NewNop()
	state := buildState(b, buildTestSnapshot(b.N), logger)
	storage := storage.New("", state, logger)

	b.ReportAllocs()
	b.ResetTimer()

	_, err := storage.Export(io.Discard)
	require.NoError(b, err)
}

func testBenchmarkAllocs(t *testing.T, f func(b *testing.B), threshold int64) {
	res := testing.Benchmark(f)

	allocs := res.AllocsPerOp()
	if allocs > threshold {
		t.Fatalf("Expected AllocsPerOp <= %d, got %d", threshold, allocs)
	}
}

func TestBenchmarkExportAllocs(t *testing.T) {
	testBenchmarkAllocs(t, BenchmarkExport, 0)
}

func buildState(tb testing.TB, data *storagepb.StateSnapshot, logger *zap.Logger) *state.State {
	i := 0
	state := state.NewState(logger)

	err := state.ImportClusterSnapshots(func() (*storagepb.ClusterSnapshot, bool, error) {
		if i >= len(data.Clusters) {
			return nil, false, nil
		}

		clusterSnapshot := data.Clusters[i]
		i++

		return clusterSnapshot, true, nil
	})
	require.NoError(tb, err)

	return state
}
