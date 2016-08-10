# Pull & push logic and tasks

from tornado import gen
import logging

logger = logging.getLogger('tornado.general')

from tasks import FuzzyRecurrentTask, register_task
from repositories import graphdb, ush_v3
import state

# Pull Themes (aka Stories) from GraphDB
class PullThemesGraphDb(object):
  def __init__(self, channel, chunk_size=100):
    self.channel = channel
    self.chunk_size = chunk_size
    self.last_date = None

  def _save_last_update(self, datetime):
    state.set("pull_themes_graphdb_channel_%s.last_update" % self.channel._id, datetime)

  @gen.coroutine
  def _get_last_update(self):
    yield state.get("pull_themes_graphdb_channel_%s.last_update" % self.channel._id)

  @gen.coroutine
  def pull(self):   # pull chunk
    # Try to pull the last update datetime, if we don't have it (this may still be None after executing)
    if self.last_date is None:
      self.last_date = yield self._get_last_update()
    #
    stories = yield graphdb.Story.fetch_updated_since(self.channel, since=self.last_date, limit=10)
    raise gen.Return(stories)

  def set_consumed(self, chunk):
    if len(chunk) > 0:
      self.last_date = chunk[-1].last_activity
      self._save_last_update(self.last_date)


# Push Themes (aka Stories) to ushahidi v3
class PushThemesUshV3(object):
  @gen.coroutine
  def push(self, chunk):    # push chunk
    for story in chunk:
      # Convert data model to V3 and save
      featured_tweet = yield story.get_featured_tweet()
      controversiality = yield story.get_controversiality_score()
      v3_story = ush_v3.Story.as_copy(story, featured_tweet=featured_tweet, controversiality=controversiality)
      yield v3_story.save()


# Remember to set
#   * task_id : to identify this task instance from others
#   * period: run pull/push action every N seconds
#   * first_delay: wait N seconds for the first run
class PullPushTask(FuzzyRecurrentTask):
  def __init__(self, pull, push, **kwargs):
    super(PullPushTask, self).__init__(**kwargs)
    self.pull = pull
    self.push = push

  @gen.coroutine
  def workload(self):
    logger.info("[%s] doing pull/push" % self.task_id)
    chunk = yield self.pull.pull()
    yield self.push.push(chunk)
    self.pull.set_consumed(chunk)
    logger.info("[%s] pull/push finished" % self.task_id)


def create_themes_pull_task(channel, **kwargs):
  task_id = "themes_pull_task_%s" % channel._id
  period = kwargs['period'] if 'period' in kwargs else 60
  chunk_size = kwargs['chunk_size'] if 'chunk_size' in kwargs else 100
  first_delay = kwargs['first_delay'] if 'first_delay' in kwargs else (0, 15)

  pull = PullThemesGraphDb(channel)
  push = PushThemesUshV3()
  task = PullPushTask(pull, push, task_id=task_id, period=period, chunk_size=chunk_size, first_delay=first_delay)

  return task

__all__ = [ 'create_themes_pull_task' ]
