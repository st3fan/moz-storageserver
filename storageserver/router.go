// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package storageserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/gorilla/mux"
	"github.com/st3fan/moz-storageserver/hawk"
	"github.com/st3fan/moz-tokenserver/token"
	"log"
	"net/http"
	"strconv"
)

func putEncodedObject(bucket *bolt.Bucket, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(key), data)
}

func getEncodedObject(bucket *bolt.Bucket, key string, value interface{}) error {
	data := bucket.Get([]byte(key))
	if data == nil {
		return ObjectNotFoundErr
	}
	return json.Unmarshal(data, &value)
}

//

var ObjectNotFoundErr = errors.New("Object not found")

type ObjectDatabase struct {
	db *bolt.DB
}

type CollectionInfo struct {
	LastModified float64
}

func OpenObjectDatabase(path string) (*ObjectDatabase, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	return &ObjectDatabase{db: db}, nil
}

func (odb *ObjectDatabase) Close() error {
	return odb.db.Close()
}

func (odb *ObjectDatabase) GetCollectionsInfo() (map[string]CollectionInfo, error) {
	infos := make(map[string]CollectionInfo)
	return infos, odb.db.View(func(tx *bolt.Tx) error {
		metaBucket := tx.Bucket([]byte("Collections"))
		if metaBucket == nil {
			return nil
		}
		return metaBucket.ForEach(func(k, v []byte) error {
			var collectionInfo CollectionInfo
			if err := json.Unmarshal(v, &collectionInfo); err != nil {
				return err
			}
			infos[string(k)] = collectionInfo
			return nil
		})
	})
}

func (odb *ObjectDatabase) GetObject(collectionName, objectId string) (Object, error) {
	var object Object
	return object, odb.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(collectionName))
		if bucket == nil {
			log.Printf("Cannot find object %s/%s", collectionName, objectId)
			return ObjectNotFoundErr
		}
		encodedObject := bucket.Get([]byte(objectId))
		if encodedObject == nil {
			log.Printf("")
			return ObjectNotFoundErr
		}
		return json.Unmarshal(encodedObject, &object)
	})
}

func (odb *ObjectDatabase) PutObject(collectionName string, object Object) (Object, error) {
	return object, odb.db.Update(func(tx *bolt.Tx) error {
		objectsBucket, err := tx.CreateBucketIfNotExists([]byte(collectionName))
		if err != nil {
			return err
		}

		// If the object already exists then this is an update and we need to merge
		var existingObject Object
		encodedExistingObject := objectsBucket.Get([]byte(object.Id))
		if encodedExistingObject == nil {
			if object.Modified == 0 {
				object.Modified = timestampNow()
			}
			if object.TTL == 0 {
				object.TTL = 2100000000
			}
		} else {
			if err := json.Unmarshal(encodedExistingObject, &existingObject); err != nil {
				return err
			}
			if object.Modified == 0.0 {
				object.Modified = existingObject.Modified
			}
			if object.TTL == 0 {
				object.TTL = existingObject.TTL
			}
			if object.Payload == "" {
				object.Payload = existingObject.Payload
			}
			if object.SortIndex == 0 {
				object.SortIndex = existingObject.SortIndex
			}
		}

		if err := putEncodedObject(objectsBucket, object.Id, object); err != nil {
			return err
		}

		// Update collections info

		metaBucket, err := tx.CreateBucketIfNotExists([]byte("Collections"))
		if err != nil {
			return err
		}

		if err := putEncodedObject(metaBucket, collectionName, CollectionInfo{LastModified: object.Modified}); err != nil {
			return err
		}

		return nil
	})
}

func (odb *ObjectDatabase) DeleteObject(collectionName, objectId string) error {
	return odb.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(collectionName))
		if err != nil {
			return err
		}

		encodedObject := bucket.Get([]byte(objectId))
		if encodedObject == nil {
			return ObjectNotFoundErr
		}

		return bucket.Delete([]byte(objectId))
	})
}

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
		path := fmt.Sprintf("%s/%d.db", c.config.DatabaseRootPath, credentials.Uid)
		odb, err := OpenObjectDatabase(path)
		if err != nil {
			log.Printf("Error while OpenObjectDatabase(%s): %s", path, err.Error())
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

func (c *handlerContext) GetObjectHandler(w http.ResponseWriter, r *http.Request) {
	if _, credentials, ok := hawk.Authorize(w, r, c.GetHawkCredentials); ok {
		path := fmt.Sprintf("%s/%d.db", c.config.DatabaseRootPath, credentials.Uid)
		odb, err := OpenObjectDatabase(path)
		if err != nil {
			log.Printf("Error while OpenObjectDatabase(%s): %s", path, err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer odb.Close()

		vars := mux.Vars(r)

		object, err := odb.GetObject(vars["collectionName"], vars["objectId"])
		if err != nil && err != ObjectNotFoundErr {
			log.Printf("Error while GetObject(%s, %s): %s", vars["collectionName"], vars["objectId"], err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		} else if err == ObjectNotFoundErr {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}

		encodedObject, err := json.Marshal(object)
		if err != nil {
			log.Printf("Error while json.Marshal(): %s", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(encodedObject)
	}
}

func (c *handlerContext) PutObjectHandler(w http.ResponseWriter, r *http.Request) {
	if _, credentials, ok := hawk.Authorize(w, r, c.GetHawkCredentials); ok {
		path := fmt.Sprintf("%s/%d.db", c.config.DatabaseRootPath, credentials.Uid)
		odb, err := OpenObjectDatabase(path)
		if err != nil {
			log.Printf("Error while OpenObjectDatabase(%s): %s", path, err.Error())
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
			log.Printf("Error while PutObject(%s, %#v): %s", vars["collectionName"], object, err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		timestamp := fmt.Sprintf("%.2f", savedObject.Modified)

		w.Header().Set("X-Weave-Timestamp", timestamp)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(timestamp))
	}
}

func (c *handlerContext) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	if _, credentials, ok := hawk.Authorize(w, r, c.GetHawkCredentials); ok {
		path := fmt.Sprintf("%s/%d.db", c.config.DatabaseRootPath, credentials.Uid)
		odb, err := OpenObjectDatabase(path)
		if err != nil {
			log.Printf("Error while OpenObjectDatabase(%s): %s", path, err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer odb.Close()

		vars := mux.Vars(r)

		err = odb.DeleteObject(vars["collectionName"], vars["objectId"])
		if err != nil && err != ObjectNotFoundErr {
			log.Printf("Error while DeleteObject(%s, %s): %s", vars["collectionName"], vars["objectId"], err.Error())
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

type PostObjectsResponse struct {
	Failed   map[string]string `json:"failed"`
	Modified float64           `json:"modified"`
	Success  []string          `json:"success"`
}

func (c *handlerContext) PostObjectsHandler(w http.ResponseWriter, r *http.Request) {
	if _, credentials, ok := hawk.Authorize(w, r, c.GetHawkCredentials); ok {
		vars := mux.Vars(r)

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

		var goodObjects []Object

		for i, _ := range objects {
			// TODO: Move this defaults logic into database.go
			if objects[i].Modified == 0 {
				objects[i].Modified = timestampNow()
			}
			if objects[i].TTL == 0 {
				objects[i].TTL = 2100000000
			}

			if err := objects[i].Validate(); err != nil {
				response.Failed[objects[i].Id] = err.Error()
			} else {
				goodObjects = append(goodObjects, objects[i])
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
	db, err := NewDatabaseSession("postgres://storageserver:storageserver@localhost/storageserver")
	if err != nil {
		return nil, err
	}

	context := &handlerContext{config: config, db: db}
	r.HandleFunc("/1.5/{userId}/info/collections", context.InfoCollectionsHandler).Methods("GET")
	r.HandleFunc("/1.5/{userId}/storage/{collectionName}/{objectId}", context.GetObjectHandler).Methods("GET")
	r.HandleFunc("/1.5/{userId}/storage/{collectionName}/{objectId}", context.PutObjectHandler).Methods("PUT")
	r.HandleFunc("/1.5/{userId}/storage/{collectionName}/{objectId}", context.DeleteObjectHandler).Methods("DELETE")
	r.HandleFunc("/1.5/{userId}/storage/{collectionName}", context.GetObjectsHandler).Methods("GET")
	r.HandleFunc("/1.5/{userId}/storage/{collectionName}", context.PostObjectsHandler).Methods("POST")
	r.HandleFunc("/1.5/{userId}/storage/{collectionName}", context.DeleteCollectionObjectsHandler).Methods("DELETE")
	r.HandleFunc("/1.5/{userId}/storage", context.DeleteUserObjectsHandler).Methods("DELETE")
	r.HandleFunc("/1.5/{userId}", context.DeleteUserObjectsHandler).Methods("DELETE")

	return context, nil
}
