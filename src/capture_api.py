#!/usr/bin/python

from tornado.httpclient import HTTPRequest, AsyncHTTPClient
from tornado import gen

import logging
from json import loads, dumps

CAPTURE_ENDPOINT="http://localhost:7080/CaptureREST/rest"

HTTP_USER="pheme"
HTTP_PASSWD="g3Br8x4g"

logger = logging.getLogger('tornado.general')

__all__ = [ "get_data_channels" ] 

@gen.coroutine
def get_data_channels():
  result = yield _do_request("/datachannel", method="GET")
  raise gen.Return(result)

@gen.coroutine
def get_data_channel(the_id):
  result = yield _do_request("/datachannel/%s" % the_id, method="GET")
  raise gen.Return(result)

@gen.coroutine
def create_data_channel(spec):
  # spec spec ;-) :
  #   { "name": .. name ..
  #     "description": .. description ..
  #     "startCaptureDate": .. start capture date ..
  #     "endCaptureDate": .. end capture date ..
  #     "type": "search",
  #     "dataSources": [
  #       { "twitter-source-1":
  #         { "type": "Twitter"
  #           "chronologicalOrder": true,
  #           "fromLastTweetId": false / <tweet id>,
  #           "keywords": " ... " }
  #       } ]
  #   }
  result = yield _do_request("/datachannel", method="POST", body=dumps(spec))
  raise gen.Return(result)

@gen.coroutine
def _do_request(endpoint, **kwargs):
  if not 'headers' in kwargs:
    kwargs['headers'] = {}
  
  if kwargs['method'] in [ 'POST', 'PUT' ]:
    kwargs['headers']['Content-Type'] = 'application/json'

  kwargs['headers']['Accept'] = 'application/json'

  r = HTTPRequest(CAPTURE_ENDPOINT + endpoint, **kwargs)
  http_client = AsyncHTTPClient()
  response = yield http_client.fetch(r)
  if response.body == "":
    raise gen.Return({})
  else:
    raise gen.Return(loads(response.body)) 
