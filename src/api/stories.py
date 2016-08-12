from tornado_json import gen
from tornado_json.requesthandlers import APIHandler
from tornado_json import schema
import logging

import my_json as json
import repositories.ush_v3 as ush_v3
import repositories.graphdb as graphdb

logger = logging.getLogger('tornado.general')

# TODO: put this somewhere else, but tornado_json route generation should still pick it up
class ModelAPIHandler(APIHandler):
  def write(self, chunk):
    if isinstance(chunk, dict):
      chunk = json.dumps(chunk).replace("</", "<\\/")
      self.set_header("Content-Type", "application/json; charset=UTF-8")
    else:
      return super(ModelAPIHandler, self).write()
    self._write_buffer.append(chunk)

class StoryDetailHandler(ModelAPIHandler):
  __urls__ = [ '/api/stories/(?P<story_id>[a-zA-Z0-9_\\-]+)' ]

  @gen.coroutine
  def get(self, story_id):
    story = yield ush_v3.Story.find_by_post_id(story_id)
    if not story:
      self.error("story %s is not in the database" % story_id)
      return
    graphdb_story = graphdb.Story(channel_id= story.channel_id, event_id= story.event_id)
    story = story.obj()
    story['images'] = yield graphdb_story.get_linked_images()
    story['articles'] = yield graphdb_story.get_related_articles()
    story['threads'] = yield graphdb.Thread.fetch_from_story(story)
    self.success(story)
