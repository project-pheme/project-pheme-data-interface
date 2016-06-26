from random import randint
from tornado import gen
from tornado.ioloop import IOLoop

import logging, time
from datetime import timedelta

logger = logging.getLogger('tornado.general')

# A class for tracking statistics and details about execution of a task
class TaskLog(object):
  def __init__(self, window=10):
    self.window = window
    self.errors = []
    self.n_error = 0
    self.n_success = 0
    self.n_execs = 0
    self.last_exec_begin = None
    self.last_exec_end = None
    self.total_exec_time = 0.0
    self.exec_times = []
    self.avg_exec_time = 0.0

  def start_exec(self):
    self.n_execs += 1
    self.last_exec_begin = time.time()
    self.last_exec_end = None

  def end_exec(self, error=None):
    if error:
      self.n_error += 1
      self.errors.append(error)
      self.errors = self._roll_window(self.errors)
    else:
      self.n_success += 1
    self.last_exec_end = time.time()
    exec_time = self.last_exec_end - self.last_exec_begin
    self.exec_times.append(exec_time)
    self.exec_times = self._roll_window(self.exec_times)
    self.total_exec_time += exec_time
    self.avg_exec_time = self.total_exec_time / self.n_execs

  def curr_exec_time(self):
    if self.last_exec_begin is None or self.last_exec_end is not None:
      return None
    else:
      return time.time() - self.last_exec_begin

  def _roll_window(self, l):
    return l[len(l)-self.window:len(l)]

# Base class for all recurrent tasks
class RecurrentTask(object):
  def __init__(self, **kwargs):
    self.task_id = kwargs['task_id']
    self.timeout = kwargs['timeout'] if 'timeout' in kwargs else 300
    self.log = TaskLog()
    self.started = False
    self.stopped = False
    self.current_future = None

  def start(self):
    if self.started:
      raise Exception("Already started")
    elif self.stopped:
      raise Exception("Cannot re-start a stopped task")
    else:
      self.started = True
      IOLoop.current().spawn_callback(self.loop)

  def stop(self):
    self.started = False
    self.stopped = True    

  @gen.coroutine
  def _exec(self):
    # wraps workload execution for error handling and execution log tracking
    self.log.start_exec()
    try:
      self.current_future = self.workload()
      yield gen.with_timeout(timedelta(seconds=self.timeout), self.current_future)
      self.log.end_exec()
    except Exception, e:
      import traceback
      logger.error("Exception in task %s" % self.task_id)
      logger.error(traceback.format_exc())
      self.log.end_exec(e)
    finally:
      self.current_future = None

  @gen.coroutine
  def loop(self):
    raise Exception("loop has to be overridden")

  @gen.coroutine
  def workload(self):
    raise Exception("workload has to be overriden!")


class SelfRegulatingTask(RecurrentTask):
  """
  A recurrently running task that self regulates its periodicity
    * first_delay : first wait to execute, in seconds
  """
  def __init__(self, **kwargs):
    super(SelfRegulatingTask, self).__init__(**kwargs)
    self.first_delay = kwargs['first_delay'] if 'first_delay' in kwargs else None
    self.next_exec = None

  @gen.coroutine
  def loop(self):
    if self.first_delay:
      yield gen.sleep(self.first_delay)
    while self.started:
      self.next_exec = None
      yield self._exec()
      if not self.next_exec:
        yield gen.sleep(30)
      else:
        yield gen.sleep(self.next_exec - time.time())


class FuzzyRecurrentTask(RecurrentTask):
  """
  A recurrently running task that adds some fuzz around its scheduled execution times.
  This is useful for avoiding a lot of actions happening at the same time, when several
  of these tasks are instantiated at once.
  The fuzziness will keep adding up as iterations go, and this will have the effect of
  spreading the several tasks more or less uniformly through time, even if they started
  at the same time.
  """
  def __init__(self, *args, **kwargs):
    super(FuzzyRecurrentTask, self).__init__(**kwargs)
    self.period = kwargs['period'] if 'period' in kwargs else None
    if not self.period:
      raise Exception("task requires a 'period' parameter in seconds")
    self.first_delay = kwargs['first_delay'] if 'first_delay' in kwargs else None
    # Initial delay, it can be an integer or a list/tuple of 2 (for a random pick in that range)
    if self.first_delay:
      if type(self.first_delay) in [ int, float ]:
        self.first_delay = int(self.first_delay)
      elif type(self.first_delay) in [ list, tuple ] and len(self.first_delay) == 2:
        self.first_delay = randint(int(self.first_delay[0]), int(self.first_delay[1]))
      else:
        raise Exception("Invalid first_delay specification, it must be number or binary tuple/list")

  @gen.coroutine
  def loop(self):
    if self.first_delay:
      yield gen.sleep(self.first_delay)
    while self.started:
      # The next execution will fall somewhere between (period * 0.67) and (period * 1.33)
      next_sleep = (self.period * 0.67) + randint(0, self.period * 2 / 3)
      next_period = gen.sleep(next_sleep)
      yield self._exec()
      yield next_period


# List of tasks managed by this module
task_roster = {}

def register_task(task, start=True):
  if task.task_id in task_roster:
    raise Exception("Task %s already registered" % task.task_id)
  #
  logger.info('Starting task %s' % task.task_id)
  task_roster[task.task_id] = task
  if start:
    task.start()

@gen.coroutine
def stop_and_deregister_task(task):
  pass
