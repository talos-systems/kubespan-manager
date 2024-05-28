// Copyright (c) 2024 Sidero Labs, Inc.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

package state

import (
	"slices"
	"sync"
	"time"

	"github.com/siderolabs/gen/maps"
	"github.com/siderolabs/gen/xslices"

	"github.com/siderolabs/discovery-service/pkg/limits"
)

// Cluster is a collection of affiliates.
//
// Cluster is gc'ed as all its affiliates are gone.
type Cluster struct {
	id              string
	affiliates      map[string]*Affiliate
	subscriptions   []*Subscription
	affiliatesMu    sync.Mutex
	subscriptionsMu sync.Mutex
}

// NewCluster creates new cluster with specified id.
func NewCluster(id string) *Cluster {
	return &Cluster{
		id:         id,
		affiliates: map[string]*Affiliate{},
	}
}

// ID returns the cluster id.
func (cluster *Cluster) ID() string {
	return cluster.id
}

// WithAffiliate runs a function against the affiliate.
//
// Cluster state is locked while the function is running.
func (cluster *Cluster) WithAffiliate(id string, f func(affiliate *Affiliate) error) error {
	cluster.affiliatesMu.Lock()
	defer cluster.affiliatesMu.Unlock()

	if affiliate, ok := cluster.affiliates[id]; ok {
		affiliate.ClearChanged()

		err := f(affiliate)

		if affiliate.IsChanged() {
			cluster.notify(&Notification{
				AffiliateID: id,
				Affiliate:   affiliate.Export(),
			})
		}

		return err
	}

	if len(cluster.affiliates) >= limits.ClusterAffiliatesMax {
		return ErrTooManyAffiliates
	}

	affiliate := NewAffiliate(id)
	err := f(affiliate)

	cluster.affiliates[id] = affiliate
	cluster.notify(&Notification{
		AffiliateID: id,
		Affiliate:   affiliate.Export(),
	})

	return err
}

// DeleteAffiliate deletes affiliate from the cluster.
func (cluster *Cluster) DeleteAffiliate(id string) {
	cluster.affiliatesMu.Lock()
	defer cluster.affiliatesMu.Unlock()

	if _, ok := cluster.affiliates[id]; ok {
		delete(cluster.affiliates, id)

		cluster.notify(&Notification{
			AffiliateID: id,
		})
	}
}

// List the affiliates.
//
// List provides a snapshot of the affiliates.
func (cluster *Cluster) List() []*AffiliateExport {
	cluster.affiliatesMu.Lock()
	defer cluster.affiliatesMu.Unlock()

	return xslices.Map(maps.Values(cluster.affiliates), func(affiliate *Affiliate) *AffiliateExport { return affiliate.Export() })
}

// Subscribe to the affiliate updates.
//
// Subscribe returns a snapshot of current list of affiliates and creates new Subscription.
func (cluster *Cluster) Subscribe(ch chan<- *Notification) ([]*AffiliateExport, *Subscription) {
	cluster.affiliatesMu.Lock()
	defer cluster.affiliatesMu.Unlock()
	cluster.subscriptionsMu.Lock()
	defer cluster.subscriptionsMu.Unlock()

	snapshot := xslices.Map(maps.Values(cluster.affiliates), func(affiliate *Affiliate) *AffiliateExport { return affiliate.Export() })

	subscription := &Subscription{
		cluster: cluster,
		errCh:   make(chan error, 1),
		ch:      ch,
	}

	cluster.subscriptions = append(cluster.subscriptions, subscription)

	return snapshot, subscription
}

func (cluster *Cluster) unsubscribe(subscription *Subscription) {
	cluster.subscriptionsMu.Lock()
	defer cluster.subscriptionsMu.Unlock()

	idx := slices.Index(cluster.subscriptions, subscription)

	if idx != -1 {
		cluster.subscriptions[idx] = cluster.subscriptions[len(cluster.subscriptions)-1]
		cluster.subscriptions[len(cluster.subscriptions)-1] = nil
		cluster.subscriptions = cluster.subscriptions[:len(cluster.subscriptions)-1]
	}
}

// GarbageCollect the cluster.
func (cluster *Cluster) GarbageCollect(now time.Time) (removedAffiliates int, empty bool) {
	cluster.affiliatesMu.Lock()
	defer cluster.affiliatesMu.Unlock()

	for id, affiliate := range cluster.affiliates {
		remove, changed := affiliate.GarbageCollect(now)

		if remove {
			delete(cluster.affiliates, id)

			removedAffiliates++
		}

		if changed {
			cluster.notify(&Notification{
				AffiliateID: id,
			})
		}
	}

	cluster.subscriptionsMu.Lock()
	subscriptions := len(cluster.subscriptions)
	cluster.subscriptionsMu.Unlock()

	empty = len(cluster.affiliates) == 0 && subscriptions == 0

	return
}

func (cluster *Cluster) notify(notifications ...*Notification) {
	cluster.subscriptionsMu.Lock()
	subscriptions := slices.Clone(cluster.subscriptions)
	cluster.subscriptionsMu.Unlock()

	for _, notification := range notifications {
		for _, subscription := range subscriptions {
			subscription.notify(notification)
		}
	}
}

// Stats returns the number of affiliates, endpoints and subscriptions.
func (cluster *Cluster) Stats() (affiliates, endpoints, subscriptions int) {
	cluster.affiliatesMu.Lock()

	affiliates = len(cluster.affiliates)
	for _, affiliate := range cluster.affiliates {
		endpoints += len(affiliate.endpoints)
	}

	cluster.affiliatesMu.Unlock()

	cluster.subscriptionsMu.Lock()

	subscriptions = len(cluster.subscriptions)

	cluster.subscriptionsMu.Unlock()

	return
}
