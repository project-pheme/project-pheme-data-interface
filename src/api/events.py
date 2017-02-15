from tornado import gen as tgen
from tornado_json import gen
from tornado_json.requesthandlers import APIHandler
from tornado_json import schema
from datetime import datetime, timedelta
import logging

import my_json as json
import capture_api
import repositories.ush_v3 as ush_v3
import pull_push
import state
from tasks import register_task

logger = logging.getLogger('tornado.general')

@tgen.coroutine
def add_event_from_datachannel(channel):
  ## Create the event
  event = ush_v3.Channel.create_from_datachannel(channel)
  yield ush_v3.get_link().add_event(event)
  # Start the pull/push task
  logger.info("Starting pull/push routine for event=%s (%s)" % (event._id, event.display_name))
  t = pull_push.create_themes_pull_task(event, period=10, first_delay=(0,2))
  register_task(t, start=True)
  raise tgen.Return(event)

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

  @tgen.coroutine
  def get(self):
    """
    GET events defined in the ushahidi instance
    """
    events = ush_v3.get_link().events
    data_channels = yield capture_api.get_data_channels()
    data_channels = dict(map(lambda ch: (ch['channelID'], ch), data_channels['dataChannel']))
    # filter / add data channel information
    def merge_event(event_id, event):
      event = event.obj()
      if data_channels.has_key(event_id):
        dc = data_channels[event_id]
        event['capture-status'] = dc['status']
        event['capture-end-date'] = dc['endCaptureDate']
      else:
        event['capture-status'] = None
        event['capture-end-date'] = None
      return (event_id, event)
    #
    self.success(dict(map(lambda it: merge_event(*it), events.items())))

  @schema.validate(
    input_schema={
      "type": "object",
      "properties": {
        "name": { "type": "string" },
        "description": { "type": "string" },
        "type": { "type": "string", "pattern": "^(search)|(stream)$" },
        "startCaptureDate": { "type": "string" },
        "endCaptureDate": { "type": "string" },
        "dataSources": {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
              "twitter": {
                "type": "object",
                "properties": {
                  "type": { "type": "string", "pattern": "^Twitter$" },
                  "keywords": { "type": "string" },
                  "chronologicalOrder": { "type": "boolean" }
                },
                "additionalProperties": False,
                "required": [ "type", "keywords" ]
              }
            },
            "additionalProperties": False,
            "required": [ "twitter" ]
          }
        }
      },
      "additionalProperties": False,
      "required": [ "name", "description", "dataSources" ]
    }
  )
  @gen.coroutine
  def post(self):
    """
    POST a new event to Capture and add it to the ushahidi instance

    * `name`: name of the data channel
    * `description`: description of the data channel
    * `startCaptureDate`: optional , "YYYY-mm-dd HH:MM:SS.mmm"
    * `endCaptureDate`: optional, "YYYY-mm-dd HH:MM:SS.mmm"
    * `dataSources`: array of object
        * `type`: set to `Twitter`
        * `keywords`: twitter search spec
        * `chronologicalOrder`: optional, default to false
    """
    # If startCaptureDate or endCaptureDate are not specified, come up with values for them
    if "startCaptureDate" not in self.body:
      # defaults to 1 day ago
      self.body["startCaptureDate"] = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S.000")
    #
    if "endCaptureDate" not in self.body:
      # defaults to 15 days from now
      self.body["endCaptureDate"] = (datetime.now() + timedelta(days=15)).strftime("%Y-%m-%d %H:%M:%S.000")
    #
    for ds in self.body["dataSources"]:
      if not "chronologicalOrder" in ds["twitter"]:
        ds["twitter"]["chronologicalOrder"] = False
      if not "historicalLimit" in ds["twitter"]:
        ds["twitter"]["historicalLimit"] = 1000
    #
    response = yield capture_api.create_data_channel(self.body)
    logger.info("Reply from Capture : " + str(response))
    if "state" in response and response["state"] == "200 OK" and "data" in response and "_global_id" in response["data"]:
      channel = yield capture_api.get_data_channel(response["data"]["_global_id"])
      event = yield add_event_from_datachannel(channel)
      # Pull updated data channel list from capture
      yield capture_api.get_directory.update(response["data"]["_global_id"], channel)
      self.success(event)
    else:
      self.error("Invalid response from Capture")

class EventStateHandler(ModelAPIHandler):
  __urls__ = [ '/api/event/(?P<datachannel_id>[a-zA-Z0-9_\\-]+)/state/(?P<action>[a-zA-Z]+)/?$' ]

  @gen.coroutine
  def get(self, datachannel_id, action):
    if action not in [ 'start', 'pause', 'reset' ]:
      self.fail("Unrecognizable state action %s" % action)
    else:
      if action == 'start':
        state.set("pull_themes_graphdb_channel_%s.frozen" % datachannel_id, False)
      elif action == 'pause':
        state.set("pull_themes_graphdb_channel_%s.frozen" % datachannel_id, True)
      elif action == 'reset':
        state.set("pull_themes_graphdb_channel_%s.last_update" % datachannel_id, None)
      self.success("OK")


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
        event = yield add_event_from_datachannel(channel)
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

