from tornado_json import gen
from tornado_json.requesthandlers import APIHandler
from tornado_json import schema
import logging

import my_json as json
import capture_api
import repositories.ush_v3 as ush_v3
import pull_push
from tasks import register_task

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


class EventScopeHandler(ModelAPIHandler):
  __urls__ = [ '/api/event' ]

  def get(self):
    """
    GET events defined in the ushahidi instance
    """
    events = ush_v3.get_link().events
    self.success(events)

  def post(self):
    """
    POST a new event for the ushahidi instance

    * `name`: name of the data channel
    * `description`: description of the data channel
    * `startCaptureDate`: optional , ISO8601
    * `endCaptureDate`: optional, ISO8601
    * `dataSources`: array of object
        * `type`: set to `Twitter`
        * `keywords`: twitter search spec
        * `chronologicalOrder`: optional, default to true
    """
    self.success({ "result": "test" })


class EventImportHandler(ModelAPIHandler):
  __urls__ = [ '/api/event/import' ]

  @schema.validate(
    input_schema={
      "type": "object",
      "properties": {
        "source": { "type": "string" },
        "dataChannelId": { "type": "string" }
      },
      "additionalProperties": False,
      "required": [ "source", "dataChannelId" ]
    }
  )
  @gen.coroutine
  def post(self):
    """
    POST an event to be created from an existing data channel

    * `source`: set to "capture"
    * `dataChannelId`: id of the data channel to be imported
    """
    if self.body["source"] != "capture":
      self.fail("Can't import data channel from source other than capture")
    else:
      datachannel_id = self.body["dataChannelId"]
      channel = yield capture_api.get_data_channel(datachannel_id)
      if channel == {}:
        self.error("Data channel id '%s' is not present in capture" % datachannel_id)
      else:
        event = ush_v3.Channel.create_from_datachannel(channel)
        yield ush_v3.get_link().add_event(event)
        # Start push/pull task for the event
        logger.info("Starting pull/push routine for event=%s (%s)" % (event._id, event.display_name))
        t = pull_push.create_themes_pull_task(event, period=10, first_delay=(0,2))
        register_task(t, start=True)
        self.success(event)

class CaptureDatachannelScopeHandler(APIHandler):
  __urls__ = [ '/api/event/capture' ]

  @gen.coroutine
  def get(self):
    """
    GET the data channels defined in capture
    """
    channels = yield capture_api.get_data_channels()
    self.success(channels)


class CaptureDatachannelHandler(APIHandler):
  __urls__ = [ '/api/event/capture/(?P<datachannel_id>[a-zA-Z0-9_\\-]+)/?$' ]
  def get(self, datachannel_id):
    """
    GET the data channel data
    """
    self.success({ "result": "test" })

