#!/usr/bin/python

from tornado.httpclient import HTTPRequest, AsyncHTTPClient
from tornado import gen

import logging, os, time
from json import loads, dumps
from urllib import urlencode

from tasks import SelfRegulatingTask

capture_proto = os.environ["CAPTURE_PROTO"] if 'CAPTURE_PROTO' in os.environ else "http"
capture_host = os.environ["CAPTURE_HOST"] if 'CAPTURE_HOST' in os.environ else "localhost"
capture_port = os.environ["CAPTURE_PORT"] if 'CAPTURE_PORT' in os.environ else "7080"

CAPTURE_ENDPOINT=capture_proto + "://" + capture_host + ":" + capture_port + "/CaptureREST/rest"

HTTP_USER="pheme"
HTTP_PASSWD="g3Br8x4g"

logger = logging.getLogger('tornado.general')

#__all__ = [ "get_data_channels", "get_data_channel", "create_data_channel", "update_data_channel", "live_search" ] 

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
  #           "chronologicalOrder": false,
  #           "fromLastTweetId": false / <tweet id>,
  #           "keywords": " ... " }
  #       } ]
  #   }
  logger.info("Sending to capture: \n" + dumps(spec, indent=2))
  result = yield _do_request("/datachannel", method="POST", body=dumps(spec))
  raise gen.Return(result)

@gen.coroutine
def update_data_channel(spec):
  # pass
  pass

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

# In memory list of data channels
class DatachannelDirectory(object):
  def __init__(self):
    self.directory = dict()

  @gen.coroutine
  def initialise(self):
    yield self.update()

  def get(self, dc_id):
    return self.directory.get(dc_id, None)

  def is_possibly_evolving(dc_id):
    # This function checks if a data channel has been finished capturing long time ago and,
    # thus, it's not justified to keep polling for updates in it
    if not self.directory.has_key(dc_id):
      return None
    else:
      from datetime import datetime, timedelta
      dc = self.directory[dc_id]
      end_date = datetime.datetime.strptime(dc["endCaptureDate"].split('.')[0], "%Y-%m-%d %H:%M:%S")
      return (datetime.utcnow() - end_date) > timedelta(hours=24)

  @gen.coroutine
  def update(self, dc_id = None, channel_data = None):
    if dc_id is None:
      data_channels = yield get_data_channels()
      data_channels = dict(map(lambda ch: (ch['channelID'], ch), data_channels['dataChannel']))
      for (dc_id, dc) in data_channels.iteritems():
        self.directory[dc_id] = dc
      logging.info("Datachannel Directory: loaded information for %d datachannels" % len(data_channels))
    else:
      if channel_data is None:
        channel_data = yield get_data_channel(dc_id)
      self.directory[dc_id] = channel_data
      logging.info("Datachannel Directory: updated datachannel %s" % dc_id)

# Periodically refresh the data channel directory
class DatachannelDirectoryRefreshTask(SelfRegulatingTask):
  def __init__(self, **kwargs):
    super(DatachannelDirectoryRefreshTask, self).__init__(**kwargs)

  @gen.coroutine
  def workload(self):
    try:
      yield get_directory().update()
    except Exception, e:
      import traceback
      logger.error("Error while refreshing datachannel directory")
      logger.error(traceback.format_exc())
    finally:
      # Run again after 10 minutes
      self.next_exec = time.time() + 600

# DatachannelDirectory singleton
_directory = DatachannelDirectory()
def get_directory():
  return _directory
