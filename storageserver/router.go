// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package storageserver

import (
	"github.com/gorilla/mux"
	"github.com/st3fan/moz-storageserver/hawk"
	"log"
	"net/http"
)

type handlerContext struct {
	config Config
}

func (c *handlerContext) GetHawkCredentials(id string) hawk.Credentials {
	return hawk.Credentials{Key: "keykey", Algorithm: "sha256", User: "stefan"}
}

func (c *handlerContext) InfoCollectionsHandler(w http.ResponseWriter, r *http.Request) {
	if ok, artifacts, credentials := hawk.Authorize(w, r, c.GetHawkCredentials); ok {
		log.Printf("Hawk artifacts: %+v", artifacts)
		log.Printf("Hawk credentials: %+v", credentials)
		w.Write([]byte(`Cheese`))
	}
}

func (c *handlerContext) InfoQuotaHandler(w http.ResponseWriter, r *http.Request) {
}

func (c *handlerContext) InfoCollectionUsageHandler(w http.ResponseWriter, r *http.Request) {
}

func (c *handlerContext) InfoCollectionCountsHandler(w http.ResponseWriter, r *http.Request) {
}

func (c *handlerContext) DeleteAllRecordsHandler(w http.ResponseWriter, r *http.Request) {
}

func SetupRouter(r *mux.Router, config Config) (*handlerContext, error) {
	context := &handlerContext{config: config}
	r.HandleFunc("/{nodeId}/info/collections", context.InfoCollectionsHandler).Methods("GET")
	r.HandleFunc("/{nodeId}/info/quota", context.InfoQuotaHandler).Methods("GET")
	r.HandleFunc("/{nodeId}/info/collection_usage", context.InfoCollectionUsageHandler).Methods("GET")
	r.HandleFunc("/{nodeId}/info/collection_counts", context.InfoCollectionCountsHandler).Methods("GET")
	r.HandleFunc("/{nodeId}", context.DeleteAllRecordsHandler).Methods("DELETE")
	return context, nil
}
