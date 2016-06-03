#!/usr/bin/env python

from tornado.web import RequestHandler, Application
from tornado.ioloop import IOLoop
from tornado.options import parse_command_line
from datetime import datetime
import json, signal
import logging

from model import *
from tasks import *
import repositories.ush_v3 as ush_v3
import state

logger = logging.getLogger('tornado.general')

def my_json_serializer(obj):
  if isinstance(obj, datetime):
    return obj.isoformat()
  elif obj.__hasattr__('obj'):
    return obj.obj()
  else:
    raise TypeError("Type not serializable")

class StatusHandler(RequestHandler):
  @gen.coroutine
  def get(self, **kwargs):
    self.set_header('Content-Type', 'application/json')
    self.write(json.dumps(dict(status="ok")))


def make_app():
  return Application([
    (r'/status/?', StatusHandler)
  ])


@gen.coroutine
def initialise_tasks():
  # Initialise state storage (state.db)
  state.init('state')

  # Ush V3 access token
  v3_link = ush_v3.get_link()
  expires_in = yield v3_link.get_new_token()
  t = UshV3TokenRefreshTask(v3_link, expires_in)
  logger.info("Starting V3 access token refresh task")
  t.spawn()
  
  # Channel polling
  channels = yield Channel.list()
  for channel in channels:
    logger.info("Starting story fetch task for channel %s" % channel._id)
    t = ChannelUpdateTask(channel._id)
    t.spawn()


def on_shutdown():
  state.quit()
  IOLoop.current().stop()

def main(port):
  parse_command_line()
  app = make_app()
  app.listen(port)
  #
  logger.info("Listening on %d" % port)
  #
  IOLoop.current().spawn_callback(initialise_tasks)
  signal.signal(signal.SIGINT, lambda sig, frame: IOLoop.current().add_callback_from_signal(on_shutdown))
  IOLoop.current().start()

if __name__ == "__main__":
  main(8888)
