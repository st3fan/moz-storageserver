// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package storageserver

const (
	DEFAULT_DATABASE_URL  = "postgres://storageserver:storageserver@localhost/storageserver"
	DEFAULT_SHARED_SECRET = "cheesebaconeggs"
)

type Config struct {
	DatabaseUrl  string
	SharedSecret string
}

func DefaultConfig() Config {
	return Config{
		DatabaseUrl:  DEFAULT_DATABASE_URL,
		SharedSecret: DEFAULT_SHARED_SECRET,
	}
}
