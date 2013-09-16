#!/usr/bin/env python2

import httplib
import xml.dom.minidom
import sys

class HTTPResponse(httplib.HTTPResponse):
    def __init__(self, *args, **kwargs):
        httplib.HTTPResponse.__init__(self, *args, **kwargs)
        self._cached_response = ''

    def read(self, amt=None):
        if amt is None:
            if not self._cached_response:
                self._cached_response = httplib.HTTPResponse.read(self)
            print("Cached Response")
            try:
                x = xml.dom.minidom.parseString(self._cached_response)
                print(x.toprettyxml())
            except:
                print(self._cached_response)
            return self._cached_response
        else:
            print("Raw Response")
            res = httplib.HTTPResponse.read(self, amt)
            try:
                x = xml.dom.minidom.parseString(res)
                print(x.toprettyxml())
            except:
                print(res)
            print(res)
            return res

import boto.connection
boto.connection.HTTPResponse = HTTPResponse

import boto
import boto.s3
import boto.s3.connection

conn = boto.connect_s3(
#    aws_access_key_id = 'AKIAJIKJHHDND7DQBLDQ', 
#    aws_secret_access_key = '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',
    aws_access_key_id = 'AKIAIOSFODNN7EXAMPLE', 
    aws_secret_access_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    host = "localhost",
    port = 8080,
    is_secure = False,
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    debug = 2)

if sys.argv[1] == 'buckets':
    for b in conn.get_all_buckets():
        print(b)
elif sys.argv[1] == 'paths':
    b = conn.get_bucket('aurelais.web', validate=False)
    for k in b.list(delimiter='/', prefix=sys.argv[2]):
        print(k)
elif sys.argv[1] == 'setkey':
    b = conn.get_bucket('aurelais.web', validate=False)
    k = boto.s3.key.Key(b)
    k.key = 'foo.txt'
    k.set_contents_from_string("foobarbaz\n")
elif sys.argv[1] == 'getkey':
    b = conn.get_bucket('aurelais.web', validate=False)
    k = boto.s3.key.Key(b)
    k.key = 'foo.txt'
    print(k.get_contents_as_string("foobarbaz\n"))
