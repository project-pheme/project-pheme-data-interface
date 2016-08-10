#!/usr/bin/env python

from tornado.httpclient import HTTPRequest, AsyncHTTPClient
from tornado import gen
from json import loads
from rdflib.plugins.sparql.results.jsonresults import JSONResult
from datetime import datetime

from string import Template
import logging, urllib, pytz, os

import iso8601

GRAPHDB_ENDPOINT = os.environ["GRAPHDB_ENDPOINT"] if "GRAPHDB_ENDPOINT" in os.environ else 'http://pheme.ontotext.com/graphdb/repositories/pheme'

GRAPHDB_PHEME_VERSION="v7"

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
          FILTER (xsd:integer(?eventId) > -1).
          ?a pheme:dataChannel "$data_channel_id".
          ?a pheme:version "$pheme_version".
      } GROUP BY (?eventId)
      having ((?size >= $min_tweets) && (?lastUpdate >= "$since_date"^^xsd:dateTime))
      order by ?lastUpdate
      limit $limit
    """).substitute(
      data_channel_id=channel._id,
      since_date=datetime_to_iso(since),
      pheme_version=GRAPHDB_PHEME_VERSION,
      limit=limit,
      min_tweets=min_tweets)
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
      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      PREFIX dlpo: <http://www.semanticdesktop.org/ontologies/2011/10/05/dlpo#>
      PREFIX sioc: <http://rdfs.org/sioc/ns#>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

      select ?thread ?source ?text ?userName ?userHandle ?date (count(?a) as ?countReplies) where {   
        ?a a pheme:ReplyingTweet .
        ?source a pheme:SourceTweet.
        ?source sioc:has_container ?thread.
        ?source sioc:has_creator ?creator.
        ?creator foaf:name ?userName.
        ?creator foaf:accountName ?userHandle.
        ?source pheme:createdAt ?date.
        ?source dlpo:textualContent ?text.
        ?a sioc:has_container ?thread.
        ?a pheme:eventId "$event_id".
        ?a pheme:version "$pheme_version".
      } GROUP BY ?thread ?source ?text ?userName ?userHandle ?date
      order by desc(?countReplies)
      limit 1
    """).substitute(event_id=self.event_id, pheme_version=GRAPHDB_PHEME_VERSION)
    result = yield query(q)
    assert len(result) == 1   # Because of grouping, there must always be a result row

    # If there were no real results from the query, try an alternative one
    x = iter(result).next()
    if x['text'] is None:
      logger.info("No replies / retweets in cluster, using alternative query")
      x = yield self._get_featured_tweet_alt()
    # Use results
    if x is not None and x['text'] is not None:
      logger.info("Representative tweet: " + str(x))
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
    q = Template("""
      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      PREFIX dlpo: <http://www.semanticdesktop.org/ontologies/2011/10/05/dlpo#>
      PREFIX sioc: <http://rdfs.org/sioc/ns#>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>

      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      select (MIN(?cDate) as ?date) ?imageURL (count(?imageURL) as ?countImage) where {   
          ?a pheme:createdAt ?cDate .
          ?a pheme:hasEvidentialityPicture ?imageURL .
          ?a pheme:eventId "$event_id".
          ?a pheme:version "$pheme_version"
      } group by ?imageURL
    """).substitute(event_id=self.event_id, pheme_version=GRAPHDB_PHEME_VERSION)
    result = yield query(q)

    raise gen.Return(map(lambda x: dict(
                          date= iso8601.parse_date(x['date'].decode()),
                          imgUrl= x['imageURL'].decode(),
                          count= int(x['countImage'].decode())),
                         result))

  @gen.coroutine
  def get_related_articles(self):
    q = Template("""
      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      PREFIX dlpo: <http://www.semanticdesktop.org/ontologies/2011/10/05/dlpo#>
      PREFIX sioc: <http://rdfs.org/sioc/ns#>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>

      select ?date ?text ?thread ?URL where {   
          ?a pheme:createdAt ?date.
          ?a sioc:has_container ?thread .
          ?a pheme:hasEvidentialityUrl ?URL .
          ?a dlpo:textualContent ?text.
          ?a pheme:eventId "$event_id".
          ?a pheme:version "v7"
      } order by desc(?date)
    """).substitute(event_id=self.event_id, pheme_version=GRAPHDB_PHEME_VERSION)
    result = yield query(q)

    raise gen.Return(map(lambda x: dict(
                          date= iso8601.parse_date(x['date'].decode()),
                          text= unicode(x['text']),
                          thread= x['thread'].decode(),
                          url= x['URL'].decode()),
                         result))

  @gen.coroutine
  def _get_featured_tweet_alt(self):
    # Just retrieve the oldest tweet from an event
    q = Template("""
      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
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
        ?a pheme:version "$pheme_version".
      }
      order by ?date
      limit 1
    """).substitute(event_id=self.event_id, pheme_version=GRAPHDB_PHEME_VERSION)
    result = yield query(q)
    if len(result) == 1:
      tweet = iter(result).next()
      raise gen.Return(tweet)

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
