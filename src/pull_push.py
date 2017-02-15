# Pull & push logic and tasks

from tornado import gen
import logging

logger = logging.getLogger('tornado.general')

from tasks import FuzzyRecurrentTask, register_task
from repositories import graphdb, ush_v3
import capture_api
import state
import model

# Pull Themes (aka Stories) from GraphDB
class PullThemesGraphDb(object):
  def __init__(self, channel, chunk_size=24):
    self.channel = channel
    self.chunk_size = chunk_size
    self.x_chunk_size = chunk_size  # we increase the chunk size sometimes
    self.last_date = None
    self.last_first_event_id = None

  def _save_last_update(self, datetime):
    state.set("pull_themes_graphdb_channel_%s.last_update" % self.channel._id, datetime)

  @gen.coroutine
  def _get_last_update(self):
    v = yield state.get("pull_themes_graphdb_channel_%s.last_update" % self.channel._id)
    raise gen.Return(v)

  @gen.coroutine
  def _get_frozen(self):
    v = yield state.get("pull_themes_graphdb_channel_%s.frozen" % self.channel._id)
    raise gen.Return(v)

  @gen.coroutine
  def pull(self):   # pull chunk
    # Check if the data channel (event/topic) is frozen
    frozen = yield self._get_frozen()
    if frozen:
      raise gen.Return([])
    # Pull the last update datetime
    self.last_date = yield self._get_last_update()
    #
    stories = yield graphdb.Story.fetch_updated_since(self.channel, since=self.last_date, limit=self.x_chunk_size)
    logger.info("(dc=%s) pull query received stories [%s]: " % (self.channel._id, ",".join(map(lambda x: x._id , stories))))
    raise gen.Return(stories)

  def set_consumed(self, chunk):
    if len(chunk) > 0:
      last_first_event_id = chunk[0].event_id

      if last_first_event_id == self.last_first_event_id:
        # We seem to be stuck without making progress, increase chunk size to try to wrangle through
        self.x_chunk_size = min(self.x_chunk_size * 2, 256)
        logger.info("Temporarily increasing fetch chunk size on channel id=%s to %d" % (self.channel._id, self.x_chunk_size))
      else:
        # We've made progress, restore default chunk size and save new chunk beginning
        logger.info("Restoring chunk size in channel id=%s" % self.channel._id)
        self.x_chunk_size = self.chunk_size
        self.last_first_event_id = last_first_event_id
      
      self.last_date = chunk[-1].last_activity
      self._save_last_update(self.last_date)


# Push Themes (aka Stories) to ushahidi v3
class PushThemesUshV3(object):
  def __init__(self, min_cluster_size=2):
    self.min_cluster_size = min_cluster_size

  # This is an optimisation to avoid processing stories that haven't changed
  def _size_remember(self, story):
    state.set("story_cached_data.%s.size" % story._id, int(story.size))

  @gen.coroutine
  def _size_has_grown(self, story, size):
    if size is None:
      raise Exception("Can't tell if story of unknown size has grown!")
    #
    v = yield state.get("story_cached_data.%s.size" % story._id)
    if v is None:
      raise gen.Return(True)
    else:
      raise gen.Return(int(size) > int(v))

  @gen.coroutine
  def push(self, chunk):    # push chunk
    logger.info("push task query received stories [%s]: " % (",".join(map(lambda x: x._id , chunk))))
    for story in chunk:
      # Convert data model to V3 and save
      # Fetch additional details
      xmeta = yield story.get_extended_metadata()

      # Skip clusters that are too small
      if xmeta['size'] < self.min_cluster_size:
        logger.info("Skipping story %s because it's too small (size=%d)" % (story._id, xmeta['size']))
        continue
      # Skip clusters that haven't grown
      if not self._size_has_grown(story, xmeta['size']):
        logger.info("Skipping story %s because it hasn't grown" % story._id)
        continue

      # The story may have evolved, fetch more details
      title = yield story.get_latest_title()
      featured_tweet = yield story.get_featured_tweet()
      controversiality = yield story.get_controversiality_score()
      images = yield story.get_linked_images()
      images = sorted(images, lambda x,y: y['count'] - x['count'])
      most_shared_img = images[0]['imgUrl'] if len(images) > 0 else ""
      tweet_texts = yield story.get_tweet_texts()

      # Create and save the story on the ush_v3 repository
      v3_story = ush_v3.Story.as_copy(story,
        title= title,
        size= xmeta['size'],
        start_date= xmeta['start_date'],
        img_count= xmeta['img_count'],
        pub_count= xmeta['pub_count'],
        verified_count = xmeta['verified_count'],
        featured_tweet= featured_tweet,
        controversiality= controversiality,
        most_shared_img = most_shared_img)

      yield v3_story.save()
      yield v3_story.upload_fulltext(' '.join([model.clean_text(title)] + tweet_texts))

      # Save size of the story in case we see it again (used by _size_has_grown)
      self._size_remember(v3_story)


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
    # Check if the channel may be stale
    # dc_id = self.pull.channel._id
    # if not capture_api.get_directory().is_possibly_evolving(dc_id):
    #   raise gen.Return()
    #
    logger.info("[%s] doing pull/push" % self.task_id)
    chunk = yield self.pull.pull()
    yield self.push.push(chunk)
    self.pull.set_consumed(chunk)
    logger.info("[%s] pull/push finished" % self.task_id)


def create_themes_pull_task(channel, **kwargs):
  logger.info("Creating task on channel %s with args %s" % (channel, str(kwargs)))

  task_id = "themes_pull_task_%s" % channel._id
  period = kwargs['period'] if 'period' in kwargs else 60
  chunk_size = kwargs['chunk_size'] if 'chunk_size' in kwargs else 24
  first_delay = kwargs['first_delay'] if 'first_delay' in kwargs else (0, 15)
  min_cluster_size = kwargs['min_cluster_size'] if 'min_cluster_size' in kwargs else 2

  pull = PullThemesGraphDb(channel, chunk_size=chunk_size)
  push = PushThemesUshV3(min_cluster_size=min_cluster_size)
  task = PullPushTask(pull, push, task_id=task_id, period=period, first_delay=first_delay)

  return task

__all__ = [ 'create_themes_pull_task' ]
