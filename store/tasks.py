from random import randint
import logging

from model import *
from storage import channel_stories

logger = logging.getLogger('tornado.general')

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
      logger.info('Retrieving channel results for %s' % self.channel_id)
      stories = yield Story.find(channel, limit=25)
      # TODO: how to make this more parallel? use toro?
      result = []
      for story in stories:
        entities = yield story.get_most_mentioned_entities()
        obj = story.obj()
        obj['popular_entities'] = map(lambda e: e.obj(), entities)
        obj['popular_threads'] = yield story.get_most_popular_threads()
        result.append(obj)
      # store result
      channel_stories.put(self.channel_id, result)
      logger.info('Finished retrieving channel results for %s' % self.channel_id)
      #
      yield nxt

class StoryConsumeTask(object):
  def __init__(self, story_id):
    self.story_id = story_id
    self.frequency = 300

  @gen.coroutine
  def run(self):
    while True:
      nxt = gen.sleep(self.frequency)
      # find new threads for the story
      # ...
      # push to ushahidi instance
      # ...
      # //
      yield nxt


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

__all__ = [ 'ChannelUpdateTask' ]
