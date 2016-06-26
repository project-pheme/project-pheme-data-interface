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

class Story(model.Story):   # aka Theme / Pheme
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
                   Story(channel_id=channel._id,
                         event_id=x['eventId'].decode(),
                         size=int(x['size'].decode()),
                         start_date=iso8601.parse_date(x['startDate'].decode()),
                         last_activity=iso8601.parse_date(x['lastUpdate'].decode())
                         ), result)
    raise gen.Return(stories)

  @gen.coroutine
  def get_featured_tweet(self):
    # Grab featured tweet in Theme
    # (currently, the oldest)
    q = Template("""
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      PREFIX dlpo: <http://www.semanticdesktop.org/ontologies/2011/10/05/dlpo#>
      PREFIX sioc: <http://rdfs.org/sioc/ns#>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>

      select ?a ?text ?date ?userName ?userHandle
      where {
        ?a pheme:eventId "$event_id".
        ?a pheme:createdAt ?date.
        ?a dlpo:textualContent ?text.
        ?a sioc:has_creator ?u.
        ?u foaf:name ?userName.
        ?u foaf:accountName ?userHandle.
      }
      order by ?date
      limit 1
    """).substitute(event_id=self.event_id)
    result = yield query(q)
    if len(result) == 1:
      for x in result:
        raise gen.Return(dict(
          text= unicode(x['text']),
          date= iso8601.parse_date(x['date'].decode()),
          user= dict(
            profile_image_url = 'https://lh6.ggpht.com/Gg2BA4RXi96iE6Zi_hJdloQAZxO6lC6Drpdr7ouKAdCbEcE_Px-1o4r8bg8ku_xzyF4y=h900',
            user_description= x['userName'],
            user_screen_name= x['userHandle'])
          ))

  @gen.coroutine
  def get_linked_images(self):
    # Images appearing in the Story (cluster)
    raise Exception("not implemented")

  @gen.coroutine
  def get_related_articles(self):
    # Articles referenced from the Story (cluster) tweets
    raise Exception("not implemented")

  @gen.coroutine
  def get_author_geolocations(self):
    # Articles referenced from the Story (cluster) tweets
    raise Exception("not implemented")

  @gen.coroutine
  def get_referenced_geolocations(self):
    # Articles referenced from the Story (cluster) tweets
    raise Exception("not implemented")


_lipsum = [
  "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam vitae ligula aliquam lectus lobortis porta non a diam. Proin fringilla augue.",
  "Duis ultrices vestibulum lacus non luctus. Praesent aliquet, justo ut accumsan bibendum, sem erat sagittis nunc, a vulputate diam ulla nisi.",
  "Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit",
  "At vero eos et accusamus et iusto odio dignissimos ducimus qui blanditiis praesentium voluptatum deleniti atque corrupti" ]

class Thread(model.Thread):
  @staticmethod
  @gen.coroutine
  def fetch_from_story(story):
    ### TODO: just sample data by now ###
    from random import randint, sample
    from datetime import datetime, timedelta
    from pytz import UTC
    results = []
    for x in range(0, randint(3,10)):
      featured_tweet = dict(
        text= sample(_lipsum, 1)[0],
        date= datetime(2016,1,1,tzinfo=pytz.UTC) + timedelta(seconds=randint(0,(365*24*3600)/2)),
        user= dict(
          profile_image_url = 'https://lh6.ggpht.com/Gg2BA4RXi96iE6Zi_hJdloQAZxO6lC6Drpdr7ouKAdCbEcE_Px-1o4r8bg8ku_xzyF4y=h900',
          user_description= 'Twitter User',
          user_screen_name= 'twitteruser')
        )
      t = Thread(uri= "random%d" % x, featured_tweet=featured_tweet)
      results.append(t)
    raise gen.Return(results)
