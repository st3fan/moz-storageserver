// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package storageserver

import (
	"database/sql"
	_ "github.com/lib/pq"
)

func timestampFromInteger(ts uint64) float64 {
	return float64(ts) / float64(100) // TODO: Don't think this is correct?
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
	Id       string  `json:"id"`
	Modified float64 `json:"modified"`
	Payload  string  `json:"payload"`
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

func (ds *DatabaseSession) GetObjects(userId uint64, limit int, newer float64) ([]Object, error) {
	return nil, nil
}

func (ds *DatabaseSession) GetObjectIds(userId uint64, limit int, newer float64) ([]string, error) {
	return nil, nil
}

func (ds *DatabaseSession) DeleteAllRecords(userId uint64) error {
	_, err := ds.db.Exec("delete from Objects where UserId = $1", userId)
	return err
}
