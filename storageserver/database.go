// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package storageserver

import (
	"database/sql"
	_ "github.com/lib/pq"
	"time"
)

func timestampFromInteger(ts uint64) float64 {
	return float64(ts) / float64(100) // TODO: Don't think this is correct?
}

func integerFromTimestamp(ts float64) uint64 {
	return uint64(ts * 100)
}

func timestampNow() float64 {
	return float64(time.Now().UnixNano()/10000000) / 100
}

type DatabaseSession struct {
	url string
	db  *sql.DB
}

func NewDatabaseSession(url string) (*DatabaseSession, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return &DatabaseSession{url: url, db: db}, nil
}

func (session *DatabaseSession) Close() {
	session.db.Close()
}

func (ds *DatabaseSession) GetCollectionTimestamps(uid uint64) (map[string]float64, error) {
	rows, err := ds.db.Query("select Collectionname, max(Modified) from Objects where UserId = $1 group by CollectionName", uid)
	if err != nil {
		return nil, err
	}
	result := make(map[string]float64)
	for rows.Next() {
		var collectionName string
		var lastModified uint64
		if err := rows.Scan(&collectionName, &lastModified); err != nil {
			return nil, err
		}
		result[collectionName] = timestampFromInteger(lastModified)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

type Object struct {
	Id        string  `json:"id"`
	Modified  float64 `json:"modified"`
	Payload   string  `json:"payload"`
	SortIndex int     `json:"sortindex"`
	TTL       int     `json:"ttl"`
}

func (o *Object) Validate() error {
	return nil
}

func (ds *DatabaseSession) GetObject(userId uint64, collectionName string, objectId string) (*Object, error) {
	var modified uint64
	var object Object
	err := ds.db.QueryRow("select Id,Modified,Payload from Objects where UserId = $1 and collectionName = $2 and Id = $3", userId, collectionName, objectId).
		Scan(&object.Id, &modified, &object.Payload)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		} else {
			return nil, err
		}
	}
	object.Modified = timestampFromInteger(modified)
	return &object, nil
}

func (ds *DatabaseSession) PutObject(userId uint64, collectionName string, objectId string, object Object) (float64, error) {
	var exists bool
	if err := ds.db.QueryRow("SELECT 1 FROM Objects WHERE UserId=$1 and CollectionName=$2 and Id=$3", userId, collectionName, objectId).Scan(&exists); err != nil && err != sql.ErrNoRows {
		return 0, err
	}

	existingObject, err := ds.GetObject(userId, collectionName, objectId)
	if err != nil {
		return 0, err
	}

	if exists {
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
		if _, err := ds.db.Exec("update Objects set SortIndex=$1,Modified=$2, Payload=$3, TTL=$4 where UserId=$5 and CollectionName=$6 and Id=$7", object.SortIndex, integerFromTimestamp(object.Modified), object.Payload, object.TTL, userId, collectionName, objectId); err != nil {
			return 0, err
		}
	} else {
		if object.Modified == 0.0 {
			object.Modified = timestampNow()
		}
		if object.TTL == 0 {
			object.TTL = 2100000000
		}
		if _, err := ds.db.Exec("insert into Objects (UserId, CollectionName, Id, SortIndex, Modified, Payload, TTL) values ($1, $2, $3, $4, $5, $6, $7)", userId, collectionName, objectId, object.SortIndex, integerFromTimestamp(object.Modified), object.Payload, object.TTL); err != nil {
			return 0, err
		}
	}

	return object.Modified, nil
}

func (ds *DatabaseSession) GetObjects(userId uint64, collectionName string, limit int, newer float64) ([]Object, error) {
	if limit == 0 {
		limit = 5000
	}
	rows, err := ds.db.Query("select Id,Modified,Payload from Objects where UserId = $1 and CollectionName = $2 and Modified > $3 order by Modified limit $4", userId, collectionName, integerFromTimestamp(newer), limit)
	if err != nil {
		return nil, err
	}
	var result []Object
	for rows.Next() {
		var modified uint64
		var object Object
		if err := rows.Scan(&object.Id, &modified, &object.Payload); err != nil {
			return nil, err
		}
		object.Modified = timestampFromInteger(modified)
		result = append(result, object)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// TODO: Get rid of this because I don't think it is actually used on any device?
func (ds *DatabaseSession) GetObjectIds(userId uint64, limit int, newer float64) ([]string, error) {
	panic("GetObjectIds is not implemented. Should it?")
	return nil, nil
}

func (ds *DatabaseSession) DeleteCollectionObjects(userId uint64, collectionName string) error {
	_, err := ds.db.Exec("delete from Objects where UserId = $1 and CollectionName = $2", userId, collectionName)
	return err
}

func (ds *DatabaseSession) DeleteUserObjects(userId uint64) error {
	_, err := ds.db.Exec("delete from Objects where UserId = $1", userId)
	return err
}

func (ds *DatabaseSession) SetObjects(userId uint64, collectionName string, objects []Object) (float64, error) {
	var lastModified float64
	for _, object := range objects {
		if object.Modified > lastModified {
			lastModified = object.Modified
		}

		var exists bool
		if err := ds.db.QueryRow("SELECT 1 FROM Objects WHERE UserId=$1 and CollectionName=$2 and Id=$3", userId, collectionName, object.Id).Scan(&exists); err != nil && err != sql.ErrNoRows {
			return 0, err
		}

		if exists {
			if _, err := ds.db.Exec("update Objects set SortIndex=$1,Modified=$2, Payload=$3, TTL=$4 where UserId=$5 and CollectionName=$6 and Id=$7", object.SortIndex, integerFromTimestamp(object.Modified), object.Payload, object.TTL, userId, collectionName, object.Id); err != nil {
				return 0, err
			}
		} else {
			if _, err := ds.db.Exec("insert into Objects (UserId, CollectionName, Id, SortIndex, Modified, Payload, TTL) values ($1, $2, $3, $4, $5, $6, $7)", userId, collectionName, object.Id, object.SortIndex, integerFromTimestamp(object.Modified), object.Payload, object.TTL); err != nil {
				return 0, err
			}
		}
	}
	return lastModified, nil
}
