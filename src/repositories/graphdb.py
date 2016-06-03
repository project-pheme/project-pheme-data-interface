#!/usr/bin/env python

from tornado.httpclient import HTTPRequest, AsyncHTTPClient
from tornado import gen
from json import loads
from rdflib.plugins.sparql.results.jsonresults import JSONResult
from datetime import datetime

from string import Template
import logging, urllib, pytz

import iso8601

GRAPHDB_ENDPOINT = 'http://pheme.ontotext.com/graphdb/repositories/pheme'

logger = logging.getLogger('tornado.general')

def datetime_to_iso(dt, default=0):
  if dt is None:
      dt = datetime.utcfromtimestamp(default).replace(tzinfo=pytz.utc);
  if dt.tzinfo is None:
    raise Exception("datetime provided is tz-unaware!")
  return dt.isoformat()

@gen.coroutine
def query(query):
  logger.info("Sending query:\n%s" % query)
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

import model

class Story(model.Story):
  @staticmethod
  @gen.coroutine
  def fetch_updated_since(channel, since=None, limit=100, min_tweets=2):
    # Query graphdb for latest events belonging to the given channel
    q = Template("""
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      select ?eventId
                (MIN(?date) as ?startDate)
                (MAX(?date) as ?lastUpdate)
                (count(?a) as ?size)
      where {   
          ?a pheme:createdAt ?date.
          ?a pheme:eventId ?eventId.
          ?a pheme:topicName "pheme_en_graphdb".
      } GROUP BY (?eventId)
      having ((?size >= $min_tweets) && (?lastUpdate >= "$since_date"^^xsd:dateTime))
      order by ?lastUpdate
      limit $limit
    """).substitute(since_date=datetime_to_iso(since), limit=limit, min_tweets=min_tweets)
    result = yield query(q)
    stories = map(lambda x:
                   model.Story(channel_id=channel._id,
                         event_id=x['eventId'].decode(),
                         size=int(x['size'].decode()),
                         start_date=iso8601.parse_date(x['startDate'].decode()),
                         last_activity=iso8601.parse_date(x['lastUpdate'].decode())
                         ), result)
    raise gen.Return(stories)
