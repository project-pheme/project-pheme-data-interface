#!/usr/bin/env python

from pykafka import KafkaClient
from threading import Thread
from tornado import gen, ioloop
from tornado.queues import Queue
import random, json, time

class KafkaTopicConsumer(object):
  """
  Tornado compatible class for consuming messages from a Kafka topic. The mode of operation is executing
  the kafka consumer code into its own thread, then communicate with the tornado IO pool code through
  callbacks in the i/o loop and queues. Depends on pykafka.
  """
  def __init__(self, **kwargs):
    self.kafka_hosts = kwargs['kafka_hosts']
    self.topic_name = kwargs['topic_name']
    self.io_loop = ioloop.IOLoop.instance()
    self.message_q = Queue(maxsize=128)
    self.exit = False

    self.kafka_process = Thread(target=self._consumer_loop)
    self.kafka_process.start()

  # Bear in mind that this method is run on a separate thread !!!
  def _consumer_loop(self, **kwargs):
    print "Connecting to %s" % self.kafka_hosts
    kafka_client = KafkaClient(hosts=self.kafka_hosts)
    topic_name = self.topic_name
    topic = kafka_client.topics[topic_name]

    # Generate consumer id if necessary
    if 'consumer_id' in kwargs:
      consumer_id = kwargs['consumer_id']
    else:
      rand_id = hex(random.getrandbits(32)).rstrip("L").lstrip("0x") or "0"
      consumer_id = "ush_consumer_%s" % rand_id

    count = 0
    consumer = topic.get_simple_consumer(consumer_id, consumer_timeout_ms=1000)
    while True:
      # exit if required
      if self.exit:
        del kafka_client
        return
      # be careful with saturating the queue (queue maxsize / 2)
      if self.message_q.qsize() > 64:
        time.sleep(1)
        continue
      try:
        m = consumer.consume()
        if m is not None and m.value is not None:
          value = json.loads(m.value)
          # Pass the value to the main thread through a callback in its io loop, the call is thread-safe
          self.io_loop.add_callback(self._put, value)
          #
          count += 1
          if (count % 100) == 0:
            print "INFO: processed %d messages on topic %s" % (count, self.topic_name)
      except Exception, e:
        # TODO: more better logging
        import sys, traceback
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print "Error occurred while consuming kafka item"
        traceback.print_exception(exc_type, exc_value, exc_traceback, limit=16, file=sys.stdout)

  def _put(self, value):
    self.message_q.put(value)

  @gen.coroutine
  def get(self):
    item = yield self.message_q.get()
    self.message_q.task_done()
    raise gen.Return(item)

  def close(self):
    self.exit = True

