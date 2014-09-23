// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package storageserver

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/st3fan/moz-storageserver/hawk"
	"github.com/st3fan/moz-tokenserver/token"
	"log"
	"net/http"
	"strconv"
)

// Utility functions to deal with query parameter parsing

func parseLimit(r *http.Request) int {
	query := r.URL.Query()
	if len(query["limit"]) != 0 {
		limit, _ := strconv.Atoi(query["limit"][0])
		return limit
	}
	return 0
}

func parseFull(r *http.Request) bool {
	query := r.URL.Query()
	return len(query["full"]) != 0
}

func parseNewer(r *http.Request) float64 {
	query := r.URL.Query()
	if len(query["newer"]) != 0 {
		newer, _ := strconv.ParseFloat(query["newer"][0], 64)
		return newer
	}
	return 0
}

//

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

// Handlers

func (c *handlerContext) InfoCollectionsHandler(w http.ResponseWriter, r *http.Request) {
	if _, credentials, ok := hawk.Authorize(w, r, c.GetHawkCredentials); ok {
		collectionInfo, err := c.db.GetCollectionTimestamps(credentials.Uid)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		encodedObject, err := json.Marshal(collectionInfo)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(encodedObject)
		return
	}
}

func (c *handlerContext) GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	if _, credentials, ok := hawk.Authorize(w, r, c.GetHawkCredentials); ok {
		vars := mux.Vars(r)
		object, err := c.db.GetObject(credentials.Uid, vars["collectionName"], vars["objectId"])
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if object == nil {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}

		encodedObject, err := json.Marshal(object)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(encodedObject)
	}
}

func (c *handlerContext) GetObjectsHandler(w http.ResponseWriter, r *http.Request) {
	if _, credentials, ok := hawk.Authorize(w, r, c.GetHawkCredentials); ok {
		vars := mux.Vars(r)
		if parseFull(r) {
			objects, err := c.db.GetObjects(credentials.Uid, vars["collectionName"], parseLimit(r), parseNewer(r))
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/newlines; charset=UTF-8")
			w.Header().Set("X-Weave-Records", strconv.Itoa(len(objects)))
			if len(objects) != 0 {
				w.Header().Set("X-Weave-Timestamp", fmt.Sprintf("%.2f", objects[len(objects)-1].Modified))
			}

			for _, object := range objects {
				encodedObject, err := json.Marshal(object)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				w.Write(encodedObject)
				w.Write([]byte("\n"))
			}
		} else {
			// TODO: Get rid of this because I don't think it is actually used on any device?
			objectIds, err := c.db.GetObjectIds(credentials.Uid, parseLimit(r), parseNewer(r))
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			encodedObject, err := json.Marshal(objectIds)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(encodedObject)
		}
	}
}

type PutObjectsResponse struct {
	Failed   map[string]string `json:"failed"`
	Modified float64           `json:"modified"`
	Success  []string          `json:"success"`
}

func (c *handlerContext) PutObjectsHandler(w http.ResponseWriter, r *http.Request) {
	if _, credentials, ok := hawk.Authorize(w, r, c.GetHawkCredentials); ok {
		vars := mux.Vars(r)

		decoder := json.NewDecoder(r.Body)
		var objects []Object
		err := decoder.Decode(&objects)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		response := &PutObjectsResponse{
			Failed:   map[string]string{},
			Modified: 0,
			Success:  []string{},
		}

		var goodObjects []Object

		for _, object := range objects {
			if err := object.Validate(); err != nil {
				response.Failed[object.Id] = err.Error()
			} else {
				goodObjects = append(goodObjects, object)
			}
		}

		if response.Modified, err = c.db.SetObjects(credentials.Uid, vars["collectionName"], objects); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		for _, o := range goodObjects {
			response.Success = append(response.Success, o.Id)
		}

		encodedResponse, err := json.Marshal(response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(encodedResponse)
	}
}

func (c *handlerContext) DeleteCollectionObjectsHandler(w http.ResponseWriter, r *http.Request) {
	if _, credentials, ok := hawk.Authorize(w, r, c.GetHawkCredentials); ok {
		vars := mux.Vars(r)
		err := c.db.DeleteCollectionObjects(credentials.Uid, vars["collectionName"])
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
	}
}

func (c *handlerContext) DeleteUserObjectsHandler(w http.ResponseWriter, r *http.Request) {
	if _, credentials, ok := hawk.Authorize(w, r, c.GetHawkCredentials); ok {
		err := c.db.DeleteUserObjects(credentials.Uid)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
	}
}

func SetupRouter(r *mux.Router, config Config) (*handlerContext, error) {
	db, err := NewDatabaseSession(config.DatabaseUrl)
	if err != nil {
		return nil, err
	}

	context := &handlerContext{config: config, db: db}
	r.HandleFunc("/1.5/{userId}/info/collections", context.InfoCollectionsHandler).Methods("GET")
	r.HandleFunc("/1.5/{userId}/storage/{collectionName}/{objectId}", context.GetObjectHandler).Methods("GET")
	r.HandleFunc("/1.5/{userId}/storage/{collectionName}", context.GetObjectsHandler).Methods("GET")
	r.HandleFunc("/1.5/{userId}/storage/{collectionName}", context.PutObjectsHandler).Methods("POST")
	r.HandleFunc("/1.5/{userId}/storage/{collectionName}", context.DeleteCollectionObjectsHandler).Methods("DELETE")
	r.HandleFunc("/1.5/{userId}", context.DeleteUserObjectsHandler).Methods("DELETE")

	return context, nil
}
