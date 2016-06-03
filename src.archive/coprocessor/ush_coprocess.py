#!/usr/bin/python

import tornado.ioloop
import signal
from pheme_event_consumer import PhemeEventConsumer
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
  yield ottawa_search.ensure(with_delete=True)
  #yield trump_search.ensure(with_delete=True)


# Prepare for interruption
def on_shutdown():
  print('Shutting down')
  tornado.ioloop.IOLoop.instance().stop()


@gen.coroutine
def init():
  global ottawa_events_consumer, trump_events_consumer
  yield init_searches()
  signal.signal(signal.SIGINT, lambda sig, frame: io_loop.add_callback_from_signal(on_shutdown))

  # Create and load modules into the io loop
  #trump_events_consumer = PhemeEventConsumer("pheme_en_entities", trump_search)
  #trump_events_consumer.run(io_loop)
  ottawa_events_consumer = PhemeEventConsumer("ottawa_entities_en", ottawa_search)
  ottawa_events_consumer.run(io_loop)


io_loop = tornado.ioloop.IOLoop.current()
io_loop.add_callback(init)

# Run the io loop
io_loop.start()

# Clean up on exit
# TODO: package this up better
ottawa_events_consumer.close()
#trump_events_consumer.close()
