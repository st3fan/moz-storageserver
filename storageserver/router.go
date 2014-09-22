// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package storageserver

import (
	//"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/st3fan/moz-storageserver/hawk"
	"github.com/st3fan/moz-tokenserver/token"
	"log"
	"net/http"
)

type handlerContext struct {
	config Config
	db     *DatabaseSession
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
		Uid:           token.Payload.Uid,
	}, nil
}

//

func (c *handlerContext) InfoCollectionsHandler(w http.ResponseWriter, r *http.Request) {
	if _, credentials, ok := hawk.Authorize(w, r, c.GetHawkCredentials); ok {
		log.Printf("InfoCollectionsHandler! (%+v)", credentials)

		collectionInfo, err := c.db.GetCollectionTimestamps(credentials.Uid)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// TODO: Wow why do we still use floats for timestamp? And is there not a better way to do this?

		response := "{"
		for collectionName, lastModified := range collectionInfo {
			if len(response) != 1 {
				response += ", "
			}
			response += fmt.Sprintf(`"%s":%.2f`, collectionName, lastModified)
		}
		response += "}"

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(response))
	}
}

func (c *handlerContext) DeleteAllRecordsHandler(w http.ResponseWriter, r *http.Request) {
	if _, credentials, ok := hawk.Authorize(w, r, c.GetHawkCredentials); ok {
		log.Printf("DeleteAllRecordsHandler! (%+v)", credentials)
	}
}

func SetupRouter(r *mux.Router, config Config) (*handlerContext, error) {
	db, err := NewDatabaseSession(config.DatabaseUrl)
	if err != nil {
		return nil, err
	}

	context := &handlerContext{config: config, db: db}
	r.HandleFunc("/1.5/{userId}/info/collections", context.InfoCollectionsHandler).Methods("GET")
	r.HandleFunc("/1.5/{userId}", context.DeleteAllRecordsHandler).Methods("DELETE")

	return context, nil
}
