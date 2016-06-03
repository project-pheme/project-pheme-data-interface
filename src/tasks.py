from random import randint
from tornado import gen
from tornado.ioloop import IOLoop
import logging, time

from repositories import graphdb
from model import *
import repositories.ush_v3 as ush_v3

import state

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
    self.frequency = 5
    self.last_date = None

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
    if self.last_date is None:
      self.last_date = yield state.get("channel_update_task.last.%s" % str(self.channel_id))
    channel = yield Channel.get(self.channel_id)
    logger.info('Retrieving channel updates for %s' % self.channel_id)
    stories = yield graphdb.Story.fetch_updated_since(channel, since=self.last_date, limit=100)
    # TODO: how to make this more parallel? use toro?
    result = []
    for story in stories:
      logger.info('Got %s' % story.obj()) 
    # remember last time
    if len(stories) > 0:
      self.last_date = stories[-1].last_activity
      state.set("channel_update_task.last.%s" % str(self.channel_id), self.last_date)

  def spawn(self):
    #IOLoop.current().spawn_callback(self.run_once)
    IOLoop.current().spawn_callback(self.run)


__all__ = [ 'UshV3TokenRefreshTask', 'ChannelUpdateTask' ]
