// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package storageserver

const (
	DEFAULT_DATABASE_URL = "postgres://storageserver:storageserver@localhost/storageserver"
)

type Config struct {
	DatabaseUrl string
}

func DefaultConfig() Config {
	return Config{
		DatabaseUrl: DEFAULT_DATABASE_URL,
	}
}
