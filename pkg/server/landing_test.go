// Copyright (c) 2024 Sidero Labs, Inc.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

package server_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/siderolabs/discovery-service/internal/landing"
	internalstate "github.com/siderolabs/discovery-service/internal/state"
	"github.com/siderolabs/discovery-service/pkg/state"
)

// TestInspectHandler tests the /inspect endpoint.
// Tests at 3001 port filled with dummy cluster/affiliate data.
func TestInspectHanlder(t *testing.T) {
	logger := zaptest.NewLogger(t)
	now := time.Now()
	stateInstance := state.NewState(logger)
	testCluster := stateInstance.GetCluster("fake1")
	ctx, cancel := context.WithCancel(context.Background())

	t.Cleanup(cancel)

	// add affiliates to the cluster "fake1"
	err := testCluster.WithAffiliate("af1", func(affiliate *internalstate.Affiliate) error {
		affiliate.Update([]byte("data1"), now.Add(time.Minute))

		return nil
	})
	require.NoError(t, err)

	err = testCluster.WithAffiliate("af2", func(affiliate *internalstate.Affiliate) error {
		affiliate.Update([]byte("data2"), now.Add(time.Minute))

		return nil
	})
	require.NoError(t, err)

	ts := httptest.NewServer(landing.Handler(stateInstance, logger))
	defer ts.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/inspect?clusterID=fake1", nil)
	require.NoError(t, err)

	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	inspectPage, err := io.ReadAll(res.Body)
	require.NoError(t, err)

	err = res.Body.Close()
	require.NoError(t, err)

	assert.Equal(t, 200, res.StatusCode)

	t.Log(string(inspectPage))
}
