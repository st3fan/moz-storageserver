// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package storageserver

import (
	"encoding/json"
	"errors"
	"github.com/boltdb/bolt"
	"net/http"
)

// Errors

var CollectionNotFoundErr = errors.New("Collection not found")
var ObjectNotFoundErr = errors.New("Object not found")
var IterationCancelledErr = errors.New("Iteration cancelled")

// Utilities

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

// Object Database

type ObjectDatabase struct {
	db *bolt.DB
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

//

type CollectionInfo struct {
	LastModified float64
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

//

func (odb *ObjectDatabase) GetCollectionCounts() (map[string]int, error) {
	counts := make(map[string]int)
	return counts, odb.db.View(func(tx *bolt.Tx) error {
		metaBucket := tx.Bucket([]byte("Collections"))
		if metaBucket == nil {
			return nil
		}
		return metaBucket.ForEach(func(k, v []byte) error {
			objectsBucket := tx.Bucket(k)
			if objectsBucket != nil {
				stats := objectsBucket.Stats()
				counts[string(k)] = stats.KeyN
			}
			return nil
		})
	})
}

type GetObjectsOptions struct {
	Full   bool
	Limit  int
	Offset int
	Newer  float64
	Ids    []string
}

func ParseGetObjectsOptions(r *http.Request) (*GetObjectsOptions, error) {
	// TODO: This should also do parameter validation
	return &GetObjectsOptions{
		Full:   parseFull(r),
		Limit:  parseLimit(r),
		Offset: parseOffset(r),
		Newer:  parseNewer(r),
		Ids:    parseIds(r),
	}, nil
}

func (odb *ObjectDatabase) GetObjects(collectionName string, options *GetObjectsOptions) ([]Object, int, error) {
	objects := []Object{}
	nextOffset := 0
	return objects, nextOffset, odb.db.View(func(tx *bolt.Tx) error {
		objectsBucket := tx.Bucket([]byte(collectionName))
		if objectsBucket == nil {
			return nil
		}
		if len(options.Ids) == 0 {
			offset := 0
			err := objectsBucket.ForEach(func(k, v []byte) error {
				var object Object
				if err := getEncodedObject(objectsBucket, string(k), &object); err != nil {
					return err
				}
				if offset >= options.Offset && object.Modified > options.Newer {
					objects = append(objects, object)
					if len(objects) == options.Limit {
						stats := objectsBucket.Stats()
						if len(objects) < stats.KeyN {
							nextOffset = options.Offset + options.Limit
						}
						return IterationCancelledErr
					}
				}
				offset++
				return nil
			})
			if err == IterationCancelledErr {
				return nil
			}
			return err
		} else {
			for _, objectId := range options.Ids {
				if data := objectsBucket.Get([]byte(objectId)); data != nil {
					var object Object
					if err := json.Unmarshal(data, &object); err != nil {
						return err
					}
					if object.Modified > options.Newer {
						objects = append(objects, object)
					}
				}
			}
			return nil
		}
	})
}

func (odb *ObjectDatabase) GetObjectIds(collectionName string, options *GetObjectsOptions) ([]string, int, error) {
	nextOffset := 0
	objectIds := []string{}
	return objectIds, nextOffset, odb.db.View(func(tx *bolt.Tx) error {
		objectsBucket := tx.Bucket([]byte(collectionName))
		if objectsBucket == nil {
			return nil
		}
		if len(options.Ids) == 0 {
			offset := 0
			err := objectsBucket.ForEach(func(k, v []byte) error {
				var object Object
				if err := getEncodedObject(objectsBucket, string(k), &object); err != nil {
					return err
				}
				if offset >= options.Offset && object.Modified > options.Newer {
					objectIds = append(objectIds, string(k))
					if len(objectIds) == options.Limit {
						stats := objectsBucket.Stats()
						if len(objectIds) < stats.KeyN {
							nextOffset = options.Offset + options.Limit
						}
						return IterationCancelledErr
					}
				}
				offset++
				return nil
			})
			if err == IterationCancelledErr {
				return nil
			}
			return err
		} else {
			for _, objectId := range options.Ids {
				if data := objectsBucket.Get([]byte(objectId)); data != nil {
					var object Object
					if err := json.Unmarshal(data, &object); err != nil {
						return err
					}
					if object.Modified > options.Newer {
						objectIds = append(objectIds, objectId)
					}
				}
			}
			return nil
		}
	})
}

//

func (odb *ObjectDatabase) GetObject(collectionName, objectId string) (Object, error) {
	var object Object
	return object, odb.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(collectionName))
		if bucket == nil {
			return ObjectNotFoundErr
		}
		encodedObject := bucket.Get([]byte(objectId))
		if encodedObject == nil {
			return ObjectNotFoundErr
		}
		return json.Unmarshal(encodedObject, &object)
	})
}

//

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

//

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

//

func (odb *ObjectDatabase) DeleteObjects(collectionName string, objectIds []string) (float64, error) {
	var lastModified float64 = timestampNow()
	return lastModified, odb.db.Update(func(tx *bolt.Tx) error {
		// The bucket must exist
		bucket := tx.Bucket([]byte(collectionName))
		if bucket == nil {
			return CollectionNotFoundErr
		}
		// Delete the specified objects
		for _, objectId := range objectIds {
			if err := bucket.Delete([]byte(objectId)); err != nil {
				return err
			}
		}
		// Update meta/info
		metaBucket, err := tx.CreateBucketIfNotExists([]byte("Collections"))
		if err != nil {
			return err
		}
		return putEncodedObject(metaBucket, collectionName, CollectionInfo{LastModified: lastModified})
	})
}

func (odb *ObjectDatabase) PutObjects(collectionName string, objects []Object) (float64, error) {
	var lastModified float64 = timestampNow()
	return lastModified, odb.db.Update(func(tx *bolt.Tx) error {
		objectsBucket, err := tx.CreateBucketIfNotExists([]byte(collectionName))
		if err != nil {
			return err
		}

		for _, object := range objects {
			// If the object already exists then this is an update and we need to merge
			var existingObject Object
			encodedExistingObject := objectsBucket.Get([]byte(object.Id))
			if encodedExistingObject == nil {
				if object.TTL == 0 {
					object.TTL = 2100000000
				}
			} else {
				if err := json.Unmarshal(encodedExistingObject, &existingObject); err != nil {
					return err
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

			object.Modified = lastModified // Always set the object's modified time

			if err := putEncodedObject(objectsBucket, object.Id, object); err != nil {
				return err
			}
		}

		// Update collections info

		metaBucket, err := tx.CreateBucketIfNotExists([]byte("Collections"))
		if err != nil {
			return err
		}

		if err := putEncodedObject(metaBucket, collectionName, CollectionInfo{LastModified: lastModified}); err != nil {
			return err
		}

		return nil
	})
}

//

// Delete an entire collection. Side effects: updates the global last
// modified for the storage. Returns the global last modified. Returns
// a CollectionNotFoundErr if the collection does not exist.

func (odb *ObjectDatabase) DeleteCollection(collectionName string) (float64, error) {
	var lastModified float64
	return lastModified, odb.db.Update(func(tx *bolt.Tx) error {
		// Delete the complete bucket
		bucket := tx.Bucket([]byte("Collections"))
		if bucket == nil {
			return CollectionNotFoundErr
		}
		if err := tx.DeleteBucket([]byte(collectionName)); err != nil {
			return err
		}
		// Delete the collection from info/collections
		metaBucket, err := tx.CreateBucketIfNotExists([]byte("Collections"))
		if err != nil {
			return err
		}
		if err := metaBucket.Delete([]byte(collectionName)); err != nil {
			return err
		}
		// Find the last modified in info/collections
		return metaBucket.ForEach(func(k, v []byte) error {
			var collectionInfo CollectionInfo
			if err := json.Unmarshal(v, &collectionInfo); err != nil {
				return err
			}
			if collectionInfo.LastModified > lastModified {
				lastModified = collectionInfo.LastModified
			}
			return nil
		})
	})
}

// Delete all storage. We keep the database file but delete all collections in it.

func (odb *ObjectDatabase) DeleteStorage() error {
	return odb.db.Update(func(tx *bolt.Tx) error {
		var err error
		if metaBucket := tx.Bucket([]byte("Collections")); metaBucket != nil {
			err = metaBucket.ForEach(func(k, v []byte) error {
				return tx.DeleteBucket(k)
			})
			if err == nil {
				err = tx.DeleteBucket([]byte("Collections"))
			}
		}
		return err
	})
}
