#!/usr/bin/env python

from threading import Thread
from tornado import gen, ioloop
from tornado.queues import Queue as AsyncQueue
import random, json, time, logging, Queue
import shelve

_db = None
_cmd_q = None   # command queue (consumed by db loop)
_val_q = None   # values queue  (consumed by io loop)
_thread = None

def _db_loop():
  global _db, _cmd_q, _val_q
  io_loop = ioloop.IOLoop.instance()
  quit = False
  while not quit:
    q_cmd = None
    try:
      q_cmd = _cmd_q.get(True)
    except Queue.Empty:
      continue
    #
    if q_cmd is None or not 'cmd' in q_cmd:
      pass
    elif q_cmd['cmd'] == 'get':
      k = q_cmd['key']
      if not _db.has_key(k):
        v = None
      else:
        v = _db[k]
      io_loop.add_callback(_return_val, v)   # queue response to the io loop
    elif q_cmd['cmd'] == 'set':
      k, v = q_cmd['key'], q_cmd['value']
      _db[k] = v
      _db.sync()      # playing it safe here
    elif q_cmd['cmd'] == 'quit':
      quit = True
    # end of task
    _cmd_q.task_done()
  # end of loop
  _db.close()

def _return_val(value):
  global _val_q
  _val_q.put(value)

## -- Module interface (to use from the IO loop) --
__all__ = [ "init", "get", "set", "quit" ]

def init(path):
  global _db, _cmd_q, _val_q, _thread
  _db = shelve.open(path)
  _cmd_q = Queue.Queue() # command queue (consumed by db loop)
  _val_q = AsyncQueue()  # values queue  (consumed by io loop)
  # ... run db_loop in its own thread ...
  _thread = Thread(target=_db_loop)
  _thread.start()

@gen.coroutine
def get(k):
  global _cmd_q, _val_q
  _cmd_q.put(dict(cmd='get', key=k), True)    # Block while putting get commands to ensure consistent order
  value = yield _val_q.get()
  _val_q.task_done()
  raise gen.Return(value)

def set(k, v):
  global _cmd_q
  _cmd_q.put(dict(cmd='set', key=k, value=v), False)

def quit():
  global _cmd_q
  _cmd_q.put(dict(cmd='quit'))


if __name__ == "__main__":
  from tornado.options import parse_command_line
  import signal

  logger = logging.getLogger('tornado.general')

  @gen.coroutine
  def test():
    logger.info("Starting test")
    set('a', 1)
    logger.info("a = %d", (yield get('a')))
    logger.info("b = %s", str((yield get('b'))))
    for x in range(1,10):
      set('x%d' % x, x**2)
    for x in range(1,10):
      logger.info("x%d = %d" % (x, (yield get('x%d' % x))))

  def on_shutdown():
    quit()
    ioloop.IOLoop.current().stop()

  parse_command_line()
  init('state_test')
  ioloop.IOLoop.current().spawn_callback(test)
  signal.signal(signal.SIGINT, lambda sig, frame: ioloop.IOLoop.current().add_callback_from_signal(on_shutdown))
  ioloop.IOLoop.current().start()

