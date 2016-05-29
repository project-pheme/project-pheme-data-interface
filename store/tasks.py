from random import randint
from tornado.ioloop import IOLoop
import logging, time

from model import *
from storage import channel_stories
import ush_v3

logger = logging.getLogger('tornado.general')

class UshV3TokenRefreshTask(object):
  def __init__(self, ush_v3, expires_in):
    self.ush_link = ush_v3
    self.first_run = int(time.time()) + (expires_in * 2 / 3)
    
  @gen.coroutine
  def run(self):
    yield gen.sleep(self.first_run - time.time())
    while True:
      try:
        expires_in = yield self.ush_link.get_new_token()
        yield gen.sleep(expires_in * 2 / 3)
      except Exception, e:
        logger.error(e)
        yield gen.sleep(10)
        
  def spawn(self):
    IOLoop.current().spawn_callback(self.run)


class ChannelUpdateTask(object):
  def __init__(self, channel_id):
    self.channel_id = channel_id
    self.frequency = 300

  @gen.coroutine
  def run(self):
    channel = yield Channel.get(self.channel_id)
    while True:
      # sleep "fuzz"
      yield gen.sleep(randint(0, self.frequency * 2 / 3))
      # baseline frequency (sleep to adjust after operations)
      nxt = gen.sleep(self.frequency * 2 / 3)
      #
      try:
        yield self.run_once()
      except Exception, e:
        logger.error(e)
      #
      yield nxt

  @gen.coroutine
  def run_once(self):
    channel = yield Channel.get(self.channel_id)
    logger.info('Retrieving channel results for %s' % self.channel_id)
    stories = yield Story.find(channel, limit=100)
    # TODO: how to make this more parallel? use toro?
    result = []
    for story in stories:
      entities = yield story.get_most_mentioned_entities()
      obj = story.obj()
      obj['popular_entities'] = map(lambda e: e.obj(), entities)
      obj['popular_threads'] = yield story.get_most_popular_threads()
      main_thread = Thread(uri=obj['popular_threads'][0]['thread'])
      obj['featured_tweet'] = yield main_thread.get_originating_tweet()
      obj['featured_tweet'] = obj['featured_tweet'].obj()
      result.append(obj)
    # store result
    channel_stories.put(self.channel_id, result)
    logger.info('Finished retrieving channel results for %s' % self.channel_id)

  def spawn(self):
    IOLoop.current().spawn_callback(self.run_once)
    IOLoop.current().spawn_callback(self.run)


class StoryConsumeTask(object):
  def __init__(self, story):
    self.story = story
    self.frequency = 5
    self.last_thread_date = None

  @gen.coroutine
  def run(self):
    while True:
      # sleep "fuzz"
      yield gen.sleep(randint(0, self.frequency * 2 / 3))
      # baseline frequency (sleep to adjust after operations)
      nxt = gen.sleep(self.frequency * 2 / 3)
      #
      try:
        yield self.run_once()
      except Exception, e:
        logger.error(e)
      #
      yield nxt

  @gen.coroutine
  def run_once(self):
    # find new threads for the story
    logger.info('Consuming new threads for story %s' % self.story.event_id)
    threads = yield self.story.get_threads_since(since=self.last_thread_date, limit=50)
    self.last_thread_date = threads[-1]['created_at']
    for t in threads:
      logger.info('Got thread %s' % t['thread'].uri)
      # get first tweet for the thread
      tw = yield t['thread'].get_originating_tweet()
      logger.info('Got tweet %s' % tw.obj())
      # push to ushahidi instance

  def spawn(self):
    IOLoop.current().spawn_callback(self.run_once)
    IOLoop.current().spawn_callback(self.run)


class ThreadConsumeTweets(object):
  def __init__(self, story_id, thread_id):
    self.story_id = story_id
    self.thread_id = thread_id
    self.frequency = 300

  @gen.coroutine
  def run(self):
    while True:
      nxt = gen.sleep(self.frequency)
      # get new tweets for the thread
      # ...
      yield nxt
      
      
class ConsumeChannelThreads(object):
  def __init__(self, channel_id):
    self.channel_id = channel_id
    self.frequency = 5
    self.last_thread_date = None

  @gen.coroutine
  def run(self):
    while True:
      # sleep "fuzz"
      yield gen.sleep(randint(0, self.frequency * 2 / 3))
      # baseline frequency (sleep to adjust after operations)
      nxt = gen.sleep(self.frequency * 2 / 3)
      #
      try:
        yield self.run_once()
      except Exception, e:
        logger.error(e)
      #
      yield nxt

  @gen.coroutine
  def run_once(self):
    # find new threads for the story
    channel = yield Channel.get(self.channel_id)
    logger.info('Consuming new threads for channel %s' % self.channel_id)
    threads = yield Thread.find_on_channel(channel, since=self.last_thread_date, limit=50)
    self.last_thread_date = threads[-1]['created_at']
    for t in threads:
      logger.info('Got thread %s' % t['thread'].uri)
      # get first tweet for the thread
      tw = yield t['thread'].get_originating_tweet()
      yield t['thread'].fetch_evidentiality()
      # push to ushahidi instance
      logger.info('---')
      logger.info(str(t['thread'].obj()))
      logger.info('-')
      logger.info(str(tw.obj()))
      #
      ush_v3.get_link().create_thread_as_post(t['thread'], tw)

  def spawn(self):
    IOLoop.current().spawn_callback(self.run_once)
    IOLoop.current().spawn_callback(self.run)


__all__ = [ 'ChannelUpdateTask', 'StoryConsumeTask', 'UshV3TokenRefreshTask', 'ConsumeChannelThreads' ]
