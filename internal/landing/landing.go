// Copyright (c) 2024 Sidero Labs, Inc.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

// Package landing provides the HTML landing page.
package landing

import (
	"embed"
	"html/template"
	"io"
	"io/fs"
	"net/http"
	"net/url"

	"go.uber.org/zap"

	internalstate "github.com/siderolabs/discovery-service/internal/state"
	"github.com/siderolabs/discovery-service/pkg/state"
)

//go:embed "html/index.html"
var static embed.FS

//go:embed "templates/inspect.html.tmpl"
var inspectTemplate []byte

// ClusterInspectData represents all affiliate data asssociated with a cluster.
type ClusterInspectData struct {
	ClusterID  string
	Affiliates []*internalstate.AffiliateExport
}

var inspectPage = template.Must(template.New("inspect").Parse(string(inspectTemplate)))

// InspectHandler provides data to inspect a cluster.
func InspectHandler(w http.ResponseWriter, req *http.Request, state *state.State) error {
	clusterData := ClusterInspectData{}

	formData, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return err
	}

	clusterData.ClusterID = formData.Get("clusterID")
	fetchedCluster := state.GetCluster(clusterData.ClusterID)
	clusterData.Affiliates = fetchedCluster.List()

	return inspectPage.Execute(w, clusterData)
}

// Handler returns static landing page handler.
func Handler(state *state.State, logger *zap.Logger) http.Handler {
	subfs, err := fs.Sub(static, "html")
	if err != nil {
		panic(err)
	}

	mux := http.NewServeMux()

	mux.Handle("/", http.FileServer(http.FS(subfs)))

	// wrapper to pass state
	mux.HandleFunc("/inspect", func(w http.ResponseWriter, r *http.Request) {
		if err := InspectHandler(w, r, state); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, "Oops, try again") //nolint:errcheck
			logger.Error("failed to return the page:", zap.Error(err))
		}
	})

	return mux
}
