#!/usr/bin/env python

import json
import uuid
import time

import hawk
import requests

URL = "https://sync.sateh.com"
AUDIENCE = URL

def get_info_collections(token):
    url = URL + "/storage/1.5/2/info/collections"
    hawk_credentials = {"id": str(token["id"]), "key": str(token["key"]), "algorithm":"sha256"}
    #print "HAWK_CREDENTIALS", hawk_credentials
    hawk_header = hawk.client.header(url, "GET", {"credentials": hawk_credentials, "ext":""})
    r = requests.get(url, headers={"Authorization":hawk_header["field"]})
    r.raise_for_status()
    #print r.status_code, r.reason, r.text
    return r.json()

def get_object(token, collection_name, object_id):
    url = URL + "/storage/1.5/2/storage/%s/%s" % (collection_name, object_id)
    hawk_credentials = {"id": str(token["id"]), "key": str(token["key"]), "algorithm":"sha256"}
    #print "HAWK_CREDENTIALS", hawk_credentials
    hawk_header = hawk.client.header(url, "GET", {"credentials": hawk_credentials, "ext":""})
    r = requests.get(url, headers={"Authorization":hawk_header["field"]})
    print "  ", r.status_code, r.reason, r.text
    r.raise_for_status()
    return r.json()

def put_object(token, collection_name, o):
    url = URL + "/storage/1.5/2/storage/%s/%s" % (collection_name, o["id"])
    hawk_credentials = {"id": str(token["id"]), "key": str(token["key"]), "algorithm":"sha256"}
    #print "HAWK_CREDENTIALS", hawk_credentials
    hawk_header = hawk.client.header(url, "PUT", {"credentials": hawk_credentials, "ext":""})
    r = requests.put(url, headers={"Authorization":hawk_header["field"]}, data=json.dumps(o))
    print "  ", r.status_code, r.reason, r.text
    r.raise_for_status()
    return r.json()

def get_objects(token, collection_name, newer, limit):
    url = URL + "/storage/1.5/2/storage/%s?full=1&newer=%.2f&limit=%d" % (collection_name, newer, limit)
    hawk_credentials = {"id": str(token["id"]), "key": str(token["key"]), "algorithm":"sha256"}
    #print "HAWK_CREDENTIALS", hawk_credentials
    hawk_header = hawk.client.header(url, "GET", {"credentials": hawk_credentials, "ext":""})
    r = requests.get(url, headers={"Authorization":hawk_header["field"]})
    print r.status_code, r.reason, r.text
    print r.headers
    r.raise_for_status()
    #return r.json()

def delete_collection_objects(token, collection_name):
    url = URL + "/storage/1.5/2/storage/%s" % collection_name
    hawk_credentials = {"id": str(token["id"]), "key": str(token["key"]), "algorithm":"sha256"}
    #print "HAWK_CREDENTIALS", hawk_credentials
    hawk_header = hawk.client.header(url, "DELETE", {"credentials": hawk_credentials, "ext":""})
    r = requests.delete(url, headers={"Authorization":hawk_header["field"]})
    print r.status_code, r.reason, r.text
    r.raise_for_status()
    return r.json()

def post_collection_objects(token, collection_name, objects):
    url = URL + "/storage/1.5/2/storage/%s" % collection_name
    hawk_credentials = {"id": str(token["id"]), "key": str(token["key"]), "algorithm":"sha256"}
    hawk_header = hawk.client.header(url, "POST", {"credentials": hawk_credentials, "ext":""})
    r = requests.post(url, headers={"Authorization":hawk_header["field"]}, data=json.dumps(objects))
    print r.status_code, r.reason, r.text
    r.raise_for_status()
    return r.json()

#

def call_token_server(assertion):
    url = URL + "/token/1.0/sync/1.5"
    #url = "https://token.services.mozilla.com/1.0/sync/1.5"
    r = requests.get(url, headers={"Authorization": "BrowserID " + assertion})
    #print r.status_code, r.reason, r.text
    r.raise_for_status()
    return r.json()

def call_mockmyid_server(email, audience):
    url = "http://127.0.0.1:8080/assertion"
    r = requests.get(url, params={"email":email, "audience":audience})
    #print r.status_code, r.reason, r.text
    r.raise_for_status()
    return r.json().get("assertion")

def ts():
    return round(time.time(), 2)

if __name__ == "__main__":
    assertion = call_mockmyid_server("stefan@mockmyid.com", AUDIENCE)
    #assertion = call_mockmyid_server("stefan@mockmyid.com", "https://token.services.mozilla.com")
    print "ASSERTION", assertion
    token = call_token_server(assertion)
    print "TOKEN", token
    #print "GET_INFO_COLLECTIONS", get_info_collections(token)
    #print "GET_OBJECT", get_object(token, "tabs", "1111")
    #print "GET_OBJECTS", get_objects(token, "tabs", 1411400288.63, 0)
    #print "DELETE_COLLECTION_OBJECTS", delete_collection_objects(token, "tabs")

    objects = [
        #{"id": "abcdef", "sortindex": 100, "payload": "payload1" + str(uuid.uuid4()), "ttl": 518400, "modified": ts()},
        {"id": str(uuid.uuid4()), "sortindex": 100, "payload": "payload1", "ttl": 518400, "modified": ts()},
        {"id": str(uuid.uuid4()), "sortindex": 100, "payload": "payload2", "ttl": 518400, "modified": ts()},
        {"id": str(uuid.uuid4()), "sortindex": 100, "payload": "payload3", "ttl": 518400, "modified": ts()},
    ]

    for o in objects:
        print "PUT_COLLECTION_OBJECT", put_object(token, "things", o)
