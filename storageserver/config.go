// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package storageserver

const (
	DEFAULT_DATABASE_ROOT_PATH = "/tmp/storageserver"
	DEFAULT_SHARED_SECRET      = "cheesebaconeggs"
)

type Config struct {
	DatabaseRootPath string
	SharedSecret     string
}

func DefaultConfig() Config {
	return Config{
		DatabaseRootPath: DEFAULT_DATABASE_ROOT_PATH,
		SharedSecret:     DEFAULT_SHARED_SECRET,
	}
}
