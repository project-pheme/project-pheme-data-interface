#!/usr/bin/env python

from tornado.web import RequestHandler, Application
from tornado.ioloop import IOLoop
from tornado.options import parse_command_line
from datetime import datetime
import pytz
import json
import logging

from model import *
from storage import channel_stories, story_statuses
from tasks import *
import ush_v3

logger = logging.getLogger('tornado.general')

def my_json_serializer(obj):
  if isinstance(obj, datetime):
    return obj.isoformat()
  elif obj.__hasattr__('obj'):
    return obj.obj()
  else:
    raise TypeError("Type not serializable")

# Channels are searches that tell the system which social media should be looked for
# and brought into the system
class ChannelHandler(RequestHandler):
  @gen.coroutine
  def get(self, **kwargs):
    self.set_header('Content-Type', 'application/json')
    channels = yield Channel.list()
    ret = []
    for channel in channels:
      ret.append(channel.obj())
    self.write(json.dumps(ret))

  def post(self, **kwargs):
    # Interact with Capture API
    # Local store?
    self.write()


class StoryHandler(RequestHandler):
  @gen.coroutine
  def get(self, **kwargs):
    self.set_header('Content-Type', 'application/json')
    if 'channel_id' in kwargs:
      channel_id = kwargs['channel_id']
      self.write(json.dumps(channel_stories.get(channel_id), default=my_json_serializer))


class StoryStatusHandler(RequestHandler):
  @gen.coroutine
  def get(self, story_id, **kwargs):
    self.set_header('Content-Type', 'application/json')
    status = story_statuses.get(story_id)
    self.write(json.dumps(status))

  @gen.coroutine
  def put(self, **kwargs):
    story_id = str(kwargs['id'])
    payload = json.loads(self.request.body)
    status = story_statuses.get(story_id)
    if status.get('consumed', False) == False and payload.get('consumed', False):
      logger.info("Starting consumer for story %s" % story_id)
      story = Story(event_id=story_id)
      t = StoryConsumeTask(story)
      t.spawn()
      #
      status['consumed'] = True
      story_statuses.put(story_id, status)
    self.write(json.dumps(status))

class ThreadHandler(RequestHandler):
  @gen.coroutine
  def get(self, **kwargs):
    self.set_header('Content-Type', 'application/json')
    if 'story_id' in kwargs:
      since_ts = self.get_query_argument('since_ts', None)
      story = Story(event_id=kwargs['story_id'])
      if since_ts is not None:
        since_ts = datetime.utcfromtimestamp(int(since_ts)).replace(tzinfo=pytz.utc)
      threads = yield story.get_threads_since(since_ts)
      self.write(json.dumps(threads))
    else:
      self.write(json.dumps({}))


def make_app():
  return Application([
    (r'/channel/?', ChannelHandler),
    (r'/channel/(?P<id>\w+)', ChannelHandler),
    #
    (r'/channel/(?P<channel_id>\w+)/stories/?', StoryHandler),
    (r'/story/?', StoryHandler),
    (r'/story/(?P<id>\w+)', StoryHandler),
    #
    (r'/story/(?P<id>\w+)/status', StoryStatusHandler),
    #
    (r'/story/(?P<story_id>\w+)/threads/?', ThreadHandler),
    (r'/thread/(?P<id>\w+)', ThreadHandler)
  ])


@gen.coroutine
def initialise_tasks():
  # Ush V3 access token
  v3_link = ush_v3.get_link()
  expires_in = yield v3_link.get_new_token()
  t = UshV3TokenRefreshTask(v3_link, expires_in)
  print "Starting V3 access token refresh task"
  t.spawn()
  
  # Channel polling
  channels = yield Channel.list()
  for channel in channels:
    print "Starting thread fetch task for channel %s" % channel._id
    t = ConsumeChannelThreads(channel._id)
    t.spawn()


def main(port):
  parse_command_line()
  app = make_app()
  app.listen(port)
  #
  print "Listening on %d" % port
  #
  IOLoop.current().spawn_callback(initialise_tasks)
  IOLoop.current().start()

if __name__ == "__main__":
  main(8888)
