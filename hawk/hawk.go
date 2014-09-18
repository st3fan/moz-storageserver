// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package hawk

import (
	"encoding/base64"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"text/scanner"
)

type Credentials struct {
	Key       string
	Algorithm string
	User      string
}

type Artifacts struct {
}

var MalformedAuthorizationHeaderErr = errors.New("Malformed Authorization header")

type CredentialsFunction func(id string) Credentials

//
type Parameters struct {
	Id        string
	Timestamp int64
	Nonce     string
	Ext       string
	Mac       []byte
	Hash      []byte
}

func parseString(s string) string {
	return strings.Trim(s, `"`) // TODO: We really need to find out how strings in Hawk are encoded, maybe we need to deal with things like \t ?
}

func parseParameters(src string) (*Parameters, error) {
	items := make(map[string]string)

	s := scanner.Scanner{Mode: scanner.ScanIdents | scanner.ScanChars | scanner.ScanStrings}
	s.Init(strings.NewReader(src))

	for {
		tok := s.Scan()
		if tok != scanner.Ident {
			return nil, MalformedAuthorizationHeaderErr
		}
		name := s.TokenText()

		tok = s.Scan()
		if tok != '=' {
			return nil, MalformedAuthorizationHeaderErr
		}

		tok = s.Scan()
		if tok != scanner.String {
			return nil, MalformedAuthorizationHeaderErr
		}
		value := s.TokenText()

		items[name] = parseString(value)

		tok = s.Scan()
		if tok == scanner.EOF {
			break
		}
		if tok != ',' {
			return nil, MalformedAuthorizationHeaderErr
		}
	}

	// Now parse the items and setup a Parameters struct

	timestamp, err := strconv.ParseInt(items["ts"], 10, 64)
	if err != nil {
		return nil, err
	}

	decodedHash, err := base64.StdEncoding.DecodeString(items["hash"])
	if err != nil {
		return nil, err
	}

	decodedMac, err := base64.StdEncoding.DecodeString(items["mac"])
	if err != nil {
		return nil, err
	}

	return &Parameters{
		Id:        items["id"],
		Timestamp: timestamp,
		Nonce:     items["nonce"],
		Ext:       items["ext"],
		Hash:      decodedHash,
		Mac:       decodedMac,
	}, nil
}

func Authorize(w http.ResponseWriter, r *http.Request, f CredentialsFunction) (bool, Credentials, Artifacts) {
	// Grab the Authorization Header

	authorization := r.Header.Get("Authorization")
	if len(authorization) == 0 {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return false, Credentials{}, Artifacts{}
	}

	tokens := strings.SplitN(authorization, " ", 2)
	if len(tokens) != 2 {
		http.Error(w, "Unsupported authorization method", http.StatusUnauthorized)
		return false, Credentials{}, Artifacts{}
	}
	if tokens[0] != "Hawk" {
		http.Error(w, "Unsupported authorization method", http.StatusUnauthorized)
		return false, Credentials{}, Artifacts{}
	}

	// Parse the Hawk parameters

	parameters, err := parseParameters(tokens[1])
	if err != nil {
		http.Error(w, "Unable to parse Hawk parameters", http.StatusUnauthorized)
		return false, Credentials{}, Artifacts{}
	}

	log.Printf("Got parameters %+v", parameters)

	// Find the user and keys

	// Return the credentials and parsed artifacts

	return true, Credentials{}, Artifacts{}
}
