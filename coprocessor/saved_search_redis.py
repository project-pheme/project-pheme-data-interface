from tornado import gen
import json


class EventRanking(object):
  def __init__(self, redis_client, search_id):
    self.redis_client = redis_client
    self.search_id = search_id

  def _key(self):
    return "s:%s#ev_rank" % str(self.search_id)

  def _key_updated(self):
    return "s:%s#ev_upd" % str(self.search_id)

  @gen.coroutine
  def inc(self, event_id):
    ev_string = "ev:%s" % str(event_id)
    count = yield self.redis_client.call("ZADD", self._key(), "INCR", "1", ev_string)
    if int(count) > 100:
      import time
      curr_ts = int(time.time())
      yield self.redis_client.call("HSET", self._key_updated(), ev_string, str(curr_ts))

  @gen.coroutine
  def remove(self):
    yield self.redis_client.call("DEL", self._key())

  @gen.coroutine
  def dump(self):
    import itertools
    ranking = yield self.redis_client.call("ZREVRANGE", self._key(), "0", "24", "WITHSCORES")
    ranking = [ x for x in itertools.izip_longest(*[iter(ranking)] * 2, fillvalue="") ]
    raise gen.Return(ranking)

class TweetRanking(object):
  def __init__(self, redis_client, search_id, event_id):
    self.redis_client = redis_client
    self.search_id = search_id
    self.event_id = event_id

  def _key(self):
    return "s:%s/ev:%s#tw_rank" % (str(self.search_id), str(self.event_id))

  def _refcount_key(self):
    return "s:%s/ev:%s#tw_refcount" % (str(self.search_id), str(self.event_id))

  def _tweet_key(self, tweet):
    return "s:%s/ev:%s/tw:%s#obj" % (str(self.search_id), str(self.event_id), tweet['id'])

  @gen.coroutine
  def add(self, tweet):
    # Add to reference count
    tw_string = "tw:%s" % str(tweet['id'])
    refcount = yield self.redis_client.call("ZADD", self._refcount_key(), "INCR", "1", tw_string)
    if type(refcount) == str: refcount = int(refcount)
    # Recalculate rank and save in list of tweets (unless it's a retweet)
    if not 'retweeted_status' in tweet:
      retweet_count = tweet['retweet_count'] if 'retweet_count' in tweet else 0
      favorite_count = tweet['favorite_count'] if 'favorite_count' in tweet else 0
      score = refcount * 3.5 + retweet_count + favorite_count
      yield self.redis_client.call("ZADD", self._key(), str(score), tw_string)
    # Store tweet  (what for?)
    #yield self.redis_client.call("SET", self._tweet_key(tweet), json.dumps(tweet))

  @gen.coroutine
  def remove(self):
    yield self.redis_client.call("DEL", self._key())
    yield self.redis_client.call("DEL", self._refcount_key())

  @gen.coroutine
  def dump(self):
    import itertools
    ranking = yield self.redis_client.call("ZREVRANGE", self._key(), "0", "-1", "WITHSCORES")
    ranking = [ x for x in itertools.izip_longest(*[iter(ranking)] * 2, fillvalue="") ]
    raise gen.Return(ranking)


class TermRanking(object):
  def __init__(self, redis_client, search_id, event_id):
    self.redis_client = redis_client
    self.search_id = search_id
    self.event_id = event_id

  def _key(self):
    return "s:%s/ev:%s#term_rank" % (str(self.search_id), str(self.event_id))

  @gen.coroutine
  def add(self, tweet):
    if not 'entities' in tweet: return
    entities = tweet['entities']
    # hashtags
    if 'hashtags' in entities:
      for ht in entities['hashtags']:
        yield self.inc('#%s' % ht['text'])
    # mentions
    if 'user_mentions' in entities:
      for um in entities['user_mentions']:
        yield self.inc('@%s' % um['screen_name'])
    # pheme
    if 'pheme' in entities:
      for ph in entities['pheme']:
        yield self.inc('%%(%s,%s)' % (ph['kind'].encode('utf-8'), ph['text'].encode('utf-8')))

  @gen.coroutine
  def inc(self, term):
    yield self.redis_client.call("ZADD", self._key(), "INCR", "1", term)

  @gen.coroutine
  def remove(self):
    yield self.redis_client.call("DEL", self._key())

  @gen.coroutine
  def dump(self):
    import itertools
    ranking = yield self.redis_client.call("ZREVRANGE", self._key(), "0", "-1", "WITHSCORES")
    ranking = [ x for x in itertools.izip_longest(*[iter(ranking)] * 2, fillvalue="") ]
    raise gen.Return(ranking)


class UserRanking(object):
  def __init__(self, redis_client, search_id, event_id):
    self.redis_client = redis_client
    self.search_id = search_id
    self.event_id = event_id

  def _key(self):
    return "s:%s/ev:%s#user_rank" % (str(self.search_id), str(self.event_id))

  @gen.coroutine
  def remove(self):
    yield self.redis_client.call("DEL", self._key())


class EventRedis(object):
  def __init__(self, redis_client, search_id, event_id):
    self.redis_client = redis_client
    self.search_id = search_id
    self.event_id = event_id
    #
    self.tweet_ranking = TweetRanking(redis_client, search_id, event_id)
    self.term_ranking = TermRanking(redis_client, search_id, event_id)
    self.user_ranking = UserRanking(redis_client, search_id, event_id)

  @gen.coroutine
  def add(self, tweet):
    yield self.tweet_ranking.add(tweet)
    yield self.term_ranking.add(tweet)

  @gen.coroutine
  def remove(self):
    yield self.tweet_ranking.remove()
    yield self.term_ranking.remove()
    yield self.user_ranking.remove()

  @gen.coroutine
  def dump(self):
    tweet_ranking = yield self.tweet_ranking.dump()
    term_ranking = yield self.term_ranking.dump()
    raise gen.Return( { "tweet_rank": tweet_ranking, "term_ranking": term_ranking })


class SavedSearchRedis(object):
  __allowed_args = ( "id", "terms", "description" )
  def __init__(self, redis_client, **kwargs):
    # Copy provided args
    self.redis_client = redis_client
    for k,v in kwargs.iteritems():
      assert( k in self.__class__.__allowed_args )
      setattr(self, k, v)
    self.event_ranking = EventRanking(redis_client, self.id)
    #
    self._event_cache = {}

  def as_json(self):
    return json.dumps({ k: self.__dict__[k] for k in self.__class__.__allowed_args })

  @gen.coroutine
  def dump(self):
    ranking = yield self.event_ranking.dump()
    obj = { "event_ranking": ranking, "events": {} }
    for (event_id, rank) in ranking:
      event_id = event_id.split(":")[1]
      obj["events"][event_id] = yield self[event_id].dump()
    raise gen.Return(obj)

  def _obj_key(self):
    return "s:%s#obj" % str(self.id)

  @gen.coroutine
  def ensure(self, with_delete=False):
    if with_delete:
      yield self.remove()
    # Ensure the search is stored
    result = yield self.redis_client.call("SISMEMBER", "saved_searches", "s:%s" % str(self.id))
    if result == 0:
      result = yield self.redis_client.call("SADD", "saved_searches", "s:%s" % str(self.id))
    yield self.redis_client.call("SET", "s:%s#obj" % str(self.id), self.as_json())

  @gen.coroutine
  def count_event_in_ranking(self, event_id):
    yield self.event_ranking.inc(event_id)

  def __getitem__(self, event_id):
    if not str(event_id) in self._event_cache:
      self._event_cache[str(event_id)] = EventRedis(self.redis_client, self.id, str(event_id))
    return self._event_cache[str(event_id)]

  @gen.coroutine
  def remove(self):
    yield self.redis_client.call("SREM", "saved_searches", str(self.id))
    yield self.redis_client.call("DEL", self._obj_key())
    yield self.event_ranking.remove()