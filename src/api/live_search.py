from tornado_json import gen
from tornado_json.requesthandlers import APIHandler
import logging

import capture_api

logger = logging.getLogger('tornado.general')

class LiveSearchHandler(APIHandler):
  __urls__ = [ '/api/search/live' ]

  @gen.coroutine
  def get(self):
    keywords = self.get_query_argument("keywords")
    max_results = self.get_query_argument("max_results") or 25
    results = yield capture_api.live_search(keywords=keywords.split(' '), max_results=max_results)
    self.success(results)
