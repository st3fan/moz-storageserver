// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package storageserver

import (
	"github.com/gorilla/mux"
	"github.com/st3fan/moz-storageserver/hawk"
	"github.com/st3fan/moz-tokenserver/token"
	"log"
	"net/http"
)

type handlerContext struct {
	config Config
}

func (c *handlerContext) GetHawkCredentials(r *http.Request, keyIdentifier string) (*hawk.Credentials, error) {
	token, err := token.ParseToken([]byte(c.config.SharedSecret), keyIdentifier)
	if err != nil {
		return nil, err
	}

	return &hawk.Credentials{
		Key:           []byte(token.DerivedSecret),
		Algorithm:     "sha256",
		KeyIdentifier: keyIdentifier,
	}, nil
}

func (c *handlerContext) GetUserIdFromRequest(w http.ResponseWriter, r *http.Request) string {
	params := mux.Vars(r)
	return params["userId"]
}

func (c *handlerContext) InfoCollectionsHandler(w http.ResponseWriter, r *http.Request) {
	if _, credentials, ok := hawk.Authorize(w, r, c.GetHawkCredentials); ok {
		log.Printf("InfoCollectionsHandler! (%+v)", credentials)
	}
}

func (c *handlerContext) DeleteAllRecordsHandler(w http.ResponseWriter, r *http.Request) {
	if _, credentials, ok := hawk.Authorize(w, r, c.GetHawkCredentials); ok {
		log.Printf("DeleteAllRecordsHandler! (%+v)", credentials)
	}
}

func SetupRouter(r *mux.Router, config Config) (*handlerContext, error) {
	context := &handlerContext{config: config}
	r.HandleFunc("/1.5/{userId}/info/collections", context.InfoCollectionsHandler).Methods("GET")
	r.HandleFunc("/1.5/{userId}", context.DeleteAllRecordsHandler).Methods("DELETE")
	return context, nil
}
