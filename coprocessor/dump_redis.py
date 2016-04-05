#!/usr/bin/python

import tornado.ioloop
import signal
from saved_search_redis import SavedSearchRedis
import tornadis
from tornado import gen


@gen.coroutine
def init_searches():
  global ottawa_search, trump_search
  redis_client = tornadis.Client(host="b2d", port=6379, autoconnect=True)

  # Initialize searches in redis
  ottawa_search = SavedSearchRedis(redis_client, id="_ottw", terms="OttawaShootings", description="Ottawa Shootings")
  #trump_search = SavedSearchRedis(redis_client, id="_trump", terms="DonaldTrump", description="Donald Trump")

@gen.coroutine
def init():
  yield init_searches()
  obj = yield ottawa_search.dump()
  import json, sys
  json.dump(obj, sys.stdout)
  tornado.ioloop.IOLoop.instance().stop()

io_loop = tornado.ioloop.IOLoop.current()
io_loop.add_callback(init)

# Run the io loop
io_loop.start()
