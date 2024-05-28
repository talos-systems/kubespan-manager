// Copyright (c) 2024 Sidero Labs, Inc.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

package state

import (
	"fmt"

	storagepb "github.com/siderolabs/discovery-service/api/storage"
	internalstate "github.com/siderolabs/discovery-service/internal/state"
)

// ExportClusterSnapshots exports all cluster snapshots and calls the provided function for each one.
//
// Implements storage.Snapshotter interface.
func (state *State) ExportClusterSnapshots(f func(snapshot *storagepb.ClusterSnapshot) error) error {
	var err error

	// reuse the same snapshotin each iteration
	clusterSnapshot := &storagepb.ClusterSnapshot{}

	state.clusters.Enumerate(func(_ string, cluster *internalstate.Cluster) bool {
		cluster.Snapshot(clusterSnapshot)

		err = f(clusterSnapshot)

		return err == nil
	})

	return err
}

// ImportClusterSnapshots imports cluster snapshots by calling the provided function until it returns false.
//
// Implements storage.Snapshotter interface.
func (state *State) ImportClusterSnapshots(f func() (*storagepb.ClusterSnapshot, bool, error)) error {
	for {
		clusterSnapshot, ok, err := f()
		if err != nil {
			return err
		}

		if !ok {
			break
		}

		cluster := internalstate.ClusterFromSnapshot(clusterSnapshot)

		_, loaded := state.clusters.LoadOrStore(cluster.ID(), cluster)
		if loaded {
			return fmt.Errorf("cluster %q already exists", cluster.ID())
		}
	}

	return nil
}
