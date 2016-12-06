#!/usr/bin/python

from tornado.httpclient import HTTPRequest, AsyncHTTPClient
from tornado import gen

import logging, os
from json import loads, dumps
from urllib import urlencode


capture_proto = os.environ["CAPTURE_PROTO"] if 'CAPTURE_PROTO' in os.environ else "http"
capture_host = os.environ["CAPTURE_HOST"] if 'CAPTURE_HOST' in os.environ else "localhost"
capture_port = os.environ["CAPTURE_PORT"] if 'CAPTURE_PORT' in os.environ else "7080"

CAPTURE_ENDPOINT=capture_proto + "://" + capture_host + ":" + capture_port + "/CaptureREST/rest"

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
  #       { "twitter":
  #         { "type": "Twitter"
  #           "chronologicalOrder": true,
  #           "fromLastTweetId": false / <tweet id>,
  #           "keywords": " ... " }
  #       } ]
  #   }
  logger.info("Sending to capture: \n" + dumps(spec, indent=2))
  result = yield _do_request("/datachannel", method="POST", body=dumps(spec))
  raise gen.Return(result)


@gen.coroutine
def live_search(keywords, max_results=100):
  params = {
    "mode": "live",
    "max_results": max_results,
    "keywords": ' '.join(keywords)
  }
  result = yield _do_request("/search/tweets?%s" % urlencode(params), method="GET")
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

