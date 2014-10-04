// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package storageserver

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/st3fan/gohawk/hawk"
	"github.com/st3fan/moz-tokenserver/token"
	"net/http"
	"strconv"
	"strings"
)

const MAX_LIMIT = 5000

//

func parseLimit(r *http.Request) int {
	query := r.URL.Query()
	if len(query["limit"]) != 0 {
		limit, _ := strconv.Atoi(query["limit"][0])
		return limit
	}
	return MAX_LIMIT
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

func parseIds(r *http.Request) []string {
	query := r.URL.Query()
	if len(query["ids"]) != 0 {
		return strings.Split(query["ids"][0], ",")
	}
	return nil
}

//

type handlerContext struct {
	config         Config
	db             *DatabaseSession
	hawkAuthorizer *hawk.Authorizer
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
	if credentials, ok := c.hawkAuthorizer.Authorize(w, r); ok {
		path := fmt.Sprintf("%s/%d.db", c.config.DatabaseRootPath, credentials.Uid)
		odb, err := OpenObjectDatabase(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer odb.Close()

		collectionsInfo, err := odb.GetCollectionsInfo()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		result := make(map[string]float64)
		for collectionName, collectionInfo := range collectionsInfo {
			result[collectionName] = collectionInfo.LastModified
		}

		encodedObject, err := json.Marshal(result)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(encodedObject)
		return
	}
}

func (c *handlerContext) InfoCollectionCountsHandler(w http.ResponseWriter, r *http.Request) {
	if credentials, ok := c.hawkAuthorizer.Authorize(w, r); ok {
		path := fmt.Sprintf("%s/%d.db", c.config.DatabaseRootPath, credentials.Uid)
		odb, err := OpenObjectDatabase(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer odb.Close()

		collectionCounts, err := odb.GetCollectionCounts()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		encodedObject, err := json.Marshal(collectionCounts)
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
	if credentials, ok := c.hawkAuthorizer.Authorize(w, r); ok {
		path := fmt.Sprintf("%s/%d.db", c.config.DatabaseRootPath, credentials.Uid)
		odb, err := OpenObjectDatabase(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer odb.Close()

		vars := mux.Vars(r)

		object, err := odb.GetObject(vars["collectionName"], vars["objectId"])
		if err != nil && err != ObjectNotFoundErr {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		} else if err == ObjectNotFoundErr {
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

func (c *handlerContext) PutObjectHandler(w http.ResponseWriter, r *http.Request) {
	if credentials, ok := c.hawkAuthorizer.Authorize(w, r); ok {
		path := fmt.Sprintf("%s/%d.db", c.config.DatabaseRootPath, credentials.Uid)
		odb, err := OpenObjectDatabase(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer odb.Close()

		vars := mux.Vars(r)

		decoder := json.NewDecoder(r.Body)
		var object Object
		if err := decoder.Decode(&object); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		object.Id = vars["objectId"]

		savedObject, err := odb.PutObject(vars["collectionName"], object)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		timestamp := fmt.Sprintf("%.2f", savedObject.Modified)

		w.Header().Set("X-Weave-Timestamp", timestamp)
		w.Header().Set("X-Last-Modified", timestamp)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(timestamp))
	}
}

func (c *handlerContext) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	if credentials, ok := c.hawkAuthorizer.Authorize(w, r); ok {
		path := fmt.Sprintf("%s/%d.db", c.config.DatabaseRootPath, credentials.Uid)
		odb, err := OpenObjectDatabase(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer odb.Close()

		vars := mux.Vars(r)

		err = odb.DeleteObject(vars["collectionName"], vars["objectId"])
		if err != nil && err != ObjectNotFoundErr {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		} else if err == ObjectNotFoundErr {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
	}
}

func (c *handlerContext) GetObjectsHandler(w http.ResponseWriter, r *http.Request) {
	if credentials, ok := c.hawkAuthorizer.Authorize(w, r); ok {
		if accepts := r.Header.Get("Accepts"); accepts != "application/json" {
			http.Error(w, "Not Acceptable", http.StatusNotAcceptable)
			return
		}

		path := fmt.Sprintf("%s/%d.db", c.config.DatabaseRootPath, credentials.Uid)
		odb, err := OpenObjectDatabase(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer odb.Close()

		vars := mux.Vars(r)

		options, err := ParseGetObjectsOptions(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if options.Full {
			objects, err := odb.GetObjects(vars["collectionName"], options)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			encodedObjects, err := json.Marshal(objects)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Weave-Records", strconv.Itoa(len(objects)))
			w.Write(encodedObjects)
		} else {
			objectIds, err := odb.GetObjectIds(vars["collectionName"], options)
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
			w.Header().Set("X-Weave-Records", strconv.Itoa(len(objectIds)))
			w.Write(encodedObject)
		}
	}
}

type PostObjectsResponse struct {
	Failed   map[string]string `json:"failed"`
	Modified float64           `json:"modified"`
	Success  []string          `json:"success"`
}

func (c *handlerContext) PostObjectsHandler(w http.ResponseWriter, r *http.Request) {
	if credentials, ok := c.hawkAuthorizer.Authorize(w, r); ok {
		// We expect application/json or text/plain (from broken clients)
		if contentType := r.Header.Get("Content-Type"); contentType != "application/json" && contentType != "text/plain" {
			http.Error(w, "Not Acceptable", http.StatusUnsupportedMediaType)
			return
		}

		// Parse the incoming objects
		decoder := json.NewDecoder(r.Body)
		var objects []Object
		err := decoder.Decode(&objects)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		response := &PostObjectsResponse{
			Failed:   map[string]string{},
			Modified: 0,
			Success:  []string{},
		}

		// Collect the records that are good
		var goodObjects []Object
		for i, _ := range objects {
			if err := objects[i].Validate(); err != nil {
				response.Failed[objects[i].Id] = err.Error()
			} else {
				goodObjects = append(goodObjects, objects[i])
			}
		}

		// Insert or update the records

		path := fmt.Sprintf("%s/%d.db", c.config.DatabaseRootPath, credentials.Uid)
		odb, err := OpenObjectDatabase(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer odb.Close()

		if response.Modified, err = odb.PutObjects(mux.Vars(r)["collectionName"], objects); err != nil {
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

type DeleteCollectionObjectsResponse struct {
	Modified float64 `json:"modified"`
}

func (c *handlerContext) DeleteCollectionObjectsHandler(w http.ResponseWriter, r *http.Request) {
	if credentials, ok := c.hawkAuthorizer.Authorize(w, r); ok {
		path := fmt.Sprintf("%s/%d.db", c.config.DatabaseRootPath, credentials.Uid)
		odb, err := OpenObjectDatabase(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer odb.Close()

		vars := mux.Vars(r)

		objectIds := parseIds(r)

		var lastModified float64

		if len(objectIds) != 0 {
			lastModified, err = odb.DeleteObjects(vars["collectionName"], objectIds)
			if err != nil {
				if err == CollectionNotFoundErr {
					http.Error(w, "Collection Not Found", http.StatusNotFound)
				} else {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
			}
		} else {
			lastModified, err = odb.DeleteCollection(vars["collectionName"])
			if err != nil {
				if err == CollectionNotFoundErr {
					http.Error(w, "Collection Not Found", http.StatusNotFound)
				} else {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
			}
		}

		// Return the last modified of the collection

		response := DeleteCollectionObjectsResponse{
			Modified: lastModified,
		}

		encodedResponse, err := json.Marshal(response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		timestamp := fmt.Sprintf("%.2f", lastModified)

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Weave-Timestamp", timestamp)
		w.Header().Set("X-Last-Modified", timestamp)
		w.Write(encodedResponse)
	}
}

func (c *handlerContext) DeleteStorageHandler(w http.ResponseWriter, r *http.Request) {
	if credentials, ok := c.hawkAuthorizer.Authorize(w, r); ok {
		path := fmt.Sprintf("%s/%d.db", c.config.DatabaseRootPath, credentials.Uid)
		odb, err := OpenObjectDatabase(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer odb.Close()

		if err := odb.DeleteStorage(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
	}
}

func SetupRouter(r *mux.Router, config Config) (*handlerContext, error) {
	db, err := NewDatabaseSession("postgres://storageserver:storageserver@localhost/storageserver")
	if err != nil {
		return nil, err
	}

	context := &handlerContext{config: config, db: db}
	context.hawkAuthorizer = hawk.NewAuthorizer(context.GetHawkCredentials, hawk.NewMemoryBackedReplayChecker())

	r.HandleFunc("/1.5/{userId}/info/collections", context.InfoCollectionsHandler).Methods("GET")
	r.HandleFunc("/1.5/{userId}/info/collection_counts", context.InfoCollectionCountsHandler).Methods("GET")
	r.HandleFunc("/1.5/{userId}/storage/{collectionName}/{objectId}", context.GetObjectHandler).Methods("GET")
	r.HandleFunc("/1.5/{userId}/storage/{collectionName}/{objectId}", context.PutObjectHandler).Methods("PUT")
	r.HandleFunc("/1.5/{userId}/storage/{collectionName}/{objectId}", context.DeleteObjectHandler).Methods("DELETE")
	r.HandleFunc("/1.5/{userId}/storage/{collectionName}", context.GetObjectsHandler).Methods("GET")
	r.HandleFunc("/1.5/{userId}/storage/{collectionName}", context.PostObjectsHandler).Methods("POST")
	r.HandleFunc("/1.5/{userId}/storage/{collectionName}", context.DeleteCollectionObjectsHandler).Methods("DELETE")
	r.HandleFunc("/1.5/{userId}/storage", context.DeleteStorageHandler).Methods("DELETE")
	r.HandleFunc("/1.5/{userId}", context.DeleteStorageHandler).Methods("DELETE")

	return context, nil
}
