// Copyright (c) 2024 Sidero Labs, Inc.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

package state_test

import (
	"testing"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	internalstate "github.com/siderolabs/discovery-service/internal/state"
	"github.com/siderolabs/discovery-service/pkg/state"
)

func checkMetrics(t *testing.T, c prom.Collector) {
	problems, err := promtestutil.CollectAndLint(c)
	require.NoError(t, err)
	require.Empty(t, problems)

	assert.NotZero(t, promtestutil.CollectAndCount(c), "collector should not be unchecked")
}

func TestState(t *testing.T) {
	now := time.Now()

	st := state.NewState(zaptest.NewLogger(t))

	// Check metrics before and after the test
	// to ensure that collector does not switch from being unchecked to checked and invalid.
	checkMetrics(t, st)
	t.Cleanup(func() { checkMetrics(t, st) })

	removedClusters, removedAffiliates := st.GarbageCollect(now)
	assert.Equal(t, 0, removedClusters)
	assert.Equal(t, 0, removedAffiliates)

	st.GetCluster("id1")
	assert.NoError(t, st.GetCluster("id2").WithAffiliate("af1", func(affiliate *internalstate.Affiliate) error {
		affiliate.Update([]byte("data1"), now.Add(time.Minute))

		return nil
	}))

	removedClusters, removedAffiliates = st.GarbageCollect(now)
	assert.Equal(t, 1, removedClusters)
	assert.Equal(t, 0, removedAffiliates)

	removedClusters, removedAffiliates = st.GarbageCollect(now.Add(2 * time.Minute))
	assert.Equal(t, 1, removedClusters)
	assert.Equal(t, 1, removedAffiliates)

	removedClusters, removedAffiliates = st.GarbageCollect(now)
	assert.Equal(t, 0, removedClusters)
	assert.Equal(t, 0, removedAffiliates)
}
