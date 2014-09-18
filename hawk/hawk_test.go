// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package hawk

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func Test_parseParameters(t *testing.T) {
	parameters, err := parseParameters(`id="dh37fgj492je", ts="1353832234", nonce="j4h3g2", hash="Yi9LfIIFRtBEPt74PVmbTF/xVAwPn7ub15ePICfgnuY=", ext="some-app-ext-data", mac="aSe1DERmZuRl3pI36/9BdZmnErTw3sNzOOAUlfeKjVw="`)
	if err != nil {
		t.Error("Cannot parse Hawk parameters", err)
	}

	if parameters.Id != "dh37fgj492je" {
		t.Error("id mismatch")
	}

	if parameters.Timestamp != 1353832234 {
		t.Error("ts mismatch")
	}

	if parameters.Nonce != "j4h3g2" {
		t.Error("nonce mismatch")
	}

	if parameters.Ext != "some-app-ext-data" {
		t.Error("ext mismatch")
	}

	expectedHash, _ := hex.DecodeString("622f4b7c820546d0443edef83d599b4c5ff1540c0f9fbb9bd7978f2027e09ee6")
	if !bytes.Equal(parameters.Hash, expectedHash) {
		t.Error("mac mismatch")
	}

	expectedMac, _ := hex.DecodeString("6927b50c446666e465de9237ebff417599a712b4f0dec37338e01495f78a8d5c")
	if !bytes.Equal(parameters.Mac, expectedMac) {
		t.Error("mac mismatch")
	}
}
