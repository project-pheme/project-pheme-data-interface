from tornado_json import gen
from tornado_json.requesthandlers import APIHandler
import logging

import capture_api

logger = logging.getLogger('tornado.general')

class DataChannelsHandler(APIHandler):
  __urls__ = [ '/api/datachannels' ]

  @gen.coroutine
  def get(self):
    results = yield capture_api.get_data_channels()
    self.success(results)

class DataChannelDetailHandler(APIHandler):
  __urls__ = [ '/api/datachannels/(?P<dc_id>[a-zA-Z0-9_\\-]+)' ]

  @gen.coroutine
  def get(self, dc_id):
    results = yield capture_api.get_data_channel(dc_id)
    self.success(results)
