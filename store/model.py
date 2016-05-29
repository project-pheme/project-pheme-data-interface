#!/usr/bin/env python

_fixed_channels = {
  'trump': {'_id': 'trump', 'topic_name': 'pheme_en_graphdb', 'display_name': 'Trump'}
}

from string import Template
from tornado import gen
from datetime import datetime
import pytz
import iso8601
import graphdb

class ReadOnlyError(Exception):
  pass


class BaseModel(object):
  def __init__(self, **kwargs):
    for k in self.__class__.__slots__:
      if k in kwargs:
        object.__setattr__(self, k, kwargs[k])

  def __setattr__(self, key, value):
    raise ReadOnlyError("Can't modify %s instances!" % self.__class__)

  def obj(self):
    ret = dict()
    for k in self.__class__.__slots__:
      try:
        ret[k] = self.__getattribute__(k)
      except AttributeError, e:
        print "error on %s" % k
        print e
        pass
    return ret


class Channel(BaseModel):
  __slots__ = [ '_id', 'topic_name', 'display_name' ]

  @staticmethod
  @gen.coroutine
  def get(the_id):
    result = Channel(**(_fixed_channels[the_id]))
    raise gen.Return(result)

  @staticmethod
  @gen.coroutine
  def list():
    raise gen.Return(map(lambda c: Channel(**c), _fixed_channels.values()))

class Story(BaseModel):
  __slots__ = [ 'channel_id', 'event_id', 'tweet_count', 'last_activity' ]

  def __setattr__(self):
    raise ReadOnlyError("Can't modify Channel instances!")

  @staticmethod
  @gen.coroutine
  def find(channel, limit=100, min_tweets=5):
    # Query graphdb for latest events belonging to the given channel
    q = Template("""
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      SELECT ?eventId (MAX(?date) as ?md) (count(?a) as ?popularity) WHERE {
        ?a pheme:createdAt ?date.
        ?a pheme:eventId ?eventId.
        ?a pheme:topicName "$topic_name".
      } GROUP BY (?eventId)
      HAVING (?popularity >= $min_tweets)
      ORDER BY DESC(?md)
      LIMIT $limit
    """).substitute(topic_name=channel.topic_name, limit=limit, min_tweets=min_tweets)
    result = yield graphdb.query(q)
    events = map(lambda x:
                   Story(channel_id=channel._id,
                         event_id=x['eventId'].decode(),
                         tweet_count=x['popularity'].decode(),
                         last_activity=x['md'].decode()
                         ), result)
    raise gen.Return(events)

  @gen.coroutine
  def get_most_mentioned_entities(self, limit=10):
    channel = yield Channel.get(self.channel_id)
    q = Template("""
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      select ?instance ?name ?type (COUNT(?tweet) as ?count)
      WHERE {
       ?mention a pheme:PhemeMention.
       ?mention pheme:inst ?instance.
       ?mention pheme:name ?name.
       ?mention pheme:mentionType ?type.
       ?tweet pheme:containsMention ?mention.
       ?tweet pheme:eventId "$event_id".
       ?tweet pheme:topicName "$topic_name".
      } GROUP BY ?instance ?name ?type
      ORDER BY DESC(?count)
      LIMIT $limit
    """).substitute(event_id=self.event_id, limit=limit, topic_name=channel.topic_name)
    result = yield graphdb.query(q)
    entities = map(lambda x:
                     Entity(uri=x['instance'].decode(),
                            name=x['name'].decode(),
                            type=x['type'].decode()
                            ), result)
    raise gen.Return(entities)

  @gen.coroutine
  def get_most_popular_threads(self, limit=5):
    channel = yield Channel.get(self.channel_id)
    q = Template("""
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      PREFIX sioc: <http://rdfs.org/sioc/ns#>

      select ?thread (COUNT(?a) as ?count) where {
          ?a a pheme:Tweet.
          ?a pheme:eventId "$event_id".
          ?a pheme:topicName "$topic_name".
          ?a sioc:has_container ?thread.
      } GROUP BY ?thread
      ORDER BY DESC(?count)
      LIMIT $limit
    """).substitute(event_id=self.event_id, limit=limit, topic_name=channel.topic_name)
    result = yield graphdb.query(q)
    threads = map(lambda x: { "thread": x['thread'].decode(), "count": x['count'].decode() }, result)
    raise gen.Return(threads)

  @gen.coroutine
  def get_threads_since(self, since=None, limit=100):
    if since is None:
      since = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc);
    if since.tzinfo is None:
      raise Exception("since date provided is tz-unaware!")
    #
    q = Template("""
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      PREFIX sioc: <http://rdfs.org/sioc/ns#>

      select ?thread (MIN(?date) as ?created_at) where {
          ?a a pheme:Tweet.
          ?a pheme:eventId "$event_id".
          ?a pheme:createdAt ?date.
          ?a sioc:has_container ?thread.
      } GROUP BY ?thread
      HAVING(?created_at >= "$since_date"^^xsd:dateTime)
      ORDER BY(?created_at)
      LIMIT $limit
    """).substitute(event_id=self.event_id, since_date=since.isoformat(), limit=limit)
    result = yield graphdb.query(q)
    threads = map(lambda x: { "thread": Thread(uri=x['thread'].decode()), "created_at": iso8601.parse_date(x['created_at'].decode()) }, result)
    raise gen.Return(threads)


class Entity(BaseModel):
  __slots__ = [ 'uri', 'name', 'type' ]

  def __setattr__(self):
    raise ReadOnlyError("Can't modify Entity instances!")


class Thread(BaseModel):
  __slots__ = [ 'uri', 'created_at', 'avg_rumour_coefficient', 'evidence' ]

  def __setattr__(self):
    raise ReadOnlyError("Can't modify Thread instances!")
    
  @staticmethod
  @gen.coroutine
  def find_on_channel(channel, since=None, limit=100, min_tweets=5):
    if since is None:
      since = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc);
    if since.tzinfo is None:
      raise Exception("since date provided is tz-unaware!")
    #
    q = Template("""
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      PREFIX sioc: <http://rdfs.org/sioc/ns#>

      select ?thread (MIN(?date) as ?created_at) (AVG(?rc) as ?avg_rumour_coefficient)   where {
          ?a a pheme:Tweet.
          ?a pheme:createdAt ?date.
          ?a sioc:has_container ?thread.
          ?a pheme:rumourCoefficient ?rc.
          ?a pheme:topicName "$topic_name".
      } GROUP BY ?thread
      HAVING(?created_at >= "$since_date"^^xsd:dateTime)
      ORDER BY(?created_at)
      LIMIT $limit
    """).substitute(since_date=since.isoformat(), limit=limit, topic_name=channel.topic_name)
    result = yield graphdb.query(q)
    threads = map(lambda x: {
      "thread": Thread(uri= x['thread'].decode(),
                       created_at= iso8601.parse_date(x['created_at'].decode()),
                       avg_rumour_coefficient= x['avg_rumour_coefficient'].decode()
                       ),
      "created_at": iso8601.parse_date(x['created_at'].decode())
    }, result)
    raise gen.Return(threads)
    
  @gen.coroutine
  def fetch_evidentiality(self):
    q = Template("""
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      PREFIX sioc: <http://rdfs.org/sioc/ns#>
      PREFIX dlpo: <http://www.semanticdesktop.org/ontologies/2011/10/05/dlpo>

      select ?tweet ?evidence_uri where {
          ?tweet a pheme:Tweet.
          ?tweet sioc:has_container <$uri>.
          ?tweet pheme:hasEvidentiality ?evidence.
          ?tweet pheme:twitterEvidenceUri ?evidence_uri
      }
    """).substitute(uri=self.uri)
    result = yield graphdb.query(q)
    evidence_set = map(lambda x: { "uri": x['evidence_uri'].decode(), "tweet_ref": x['tweet'].decode() }, result)
    object.__setattr__(self, "evidence", evidence_set)
    raise gen.Return(evidence_set)

  @gen.coroutine
  def get_originating_tweet(self):
    q = Template("""
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      PREFIX sioc: <http://rdfs.org/sioc/ns#>
      PREFIX dlpo: <http://www.semanticdesktop.org/ontologies/2011/10/05/dlpo>

      select ?tweet ?date ?rc ?text where {
          ?tweet a pheme:Tweet.
          ?tweet sioc:has_container <$uri>.
          ?tweet pheme:createdAt ?date.
          ?tweet pheme:rumourCoefficient ?rc.
          ?tweet dlpo:\#textualContent ?text.
      }
      ORDER BY(?date)
      LIMIT 1
    """).substitute(uri=self.uri)
    result = yield graphdb.query(q)
    if len(result) == 0:
      raise gen.Return(None)
    else:
      for r in result: x = r
      raise gen.Return(Tweet(
        uri=x['tweet'].decode(),
        created_at=iso8601.parse_date(x['date'].decode()),
        rumour_coefficient=x['rc'].decode(),
        textual_content=x['text']
      ))


class Tweet(BaseModel):
  __slots__ = [ 'uri', 'created_at', 'rumour_coefficient', 'textual_content', ]

  def __setattr__(self):
    raise ReadOnlyError("Can't modify Tweet instances!")

  @gen.coroutine
  def get_author(self):
    q = Template("""
    """)

class User(BaseModel):
  __slots__ = [ 'uri', 'account_name', 'followers', 'friends' ]

  def __setattr__(self):
    raise ReadOnlyError("Can't modify User instances!")