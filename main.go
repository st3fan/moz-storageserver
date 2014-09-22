// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package main

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/st3fan/moz-storageserver/storageserver"
	"log"
	"net/http"
)

const (
	DEFAULT_API_PREFIX         = "/storage"
	DEFAULT_API_LISTEN_ADDRESS = "0.0.0.0"
	DEFAULT_API_LISTEN_PORT    = 8124
)

func VersionHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(`{"version":"1.0"}`)) // TODO: How can we easily embed the git rev and tag in here?
}

func main() {
	router := mux.NewRouter()
	router.HandleFunc("/version", VersionHandler)

	config := storageserver.DefaultConfig() // TODO: Get this from command line options

	_, err := storageserver.SetupRouter(router.PathPrefix(DEFAULT_API_PREFIX).Subrouter(), config)
	if err != nil {
		log.Fatal(err)
	}

	addr := fmt.Sprintf("%s:%d", DEFAULT_API_LISTEN_ADDRESS, DEFAULT_API_LISTEN_PORT)
	log.Printf("Starting storage server on http://%s", addr)
	http.Handle("/", router)
	err = http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatal(err)
	}
}
