#!/usr/bin/env python

from tornado.httpclient import HTTPRequest, AsyncHTTPClient
from tornado import gen
from json import loads
from rdflib.plugins.sparql.results.jsonresults import JSONResult

import urllib

GRAPHDB_ENDPOINT = 'http://pheme.ontotext.com/graphdb/repositories/pheme'

@gen.coroutine
def query(query):
  r = HTTPRequest(
    GRAPHDB_ENDPOINT,
    method='POST',
    headers={
      'Accept': 'application/sparql-results+json',
      'Content-Type': 'application/x-www-form-urlencoded'
      },
    body=urllib.urlencode({ 'query': query }),
    )
  http_client = AsyncHTTPClient()
  response = yield http_client.fetch(r)
  if response.error:
    raise Exception("Bad GraphDB response " + str(response))
  else:
    raise gen.Return(JSONResult(loads(response.body)))
