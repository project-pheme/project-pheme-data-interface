from tornado_json import gen
from tornado_json.requesthandlers import APIHandler
import logging, json

import capture_api

logger = logging.getLogger('tornado.general')

class LiveSearchHandler(APIHandler):
  __urls__ = [ '/api/search/live' ]

  @gen.coroutine
  def get(self):
    keywords = self.get_query_argument("keywords")
    max_results = self.get_query_argument("max_results") or 25
    results = yield capture_api.live_search(keywords=keywords.split(' '), max_results=max_results)
    # Function to post process results before returning to client
    def process_result(result):
      raw_json = result.pop("rawJson", None)
      try:
        if raw_json is not None:
          raw_tweet = json.loads(raw_json)
          result["userName"] = raw_tweet["user"]["name"]
      except KeyError, e:
        None
      return result
    ##
    self.success(map(process_result, results))
