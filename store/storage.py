
# TODO: actually store this somewhere (redis? ES?)
# Elastic Search:
# obtain results for the channel and save in storage
#
# /{site_idx}/channels : {
#   "_id": "trump",
#   "story_preview": {}
# }
# ...
# later on (to allow resorting)...
# /{site_idx}/stories : {
#   "channel_id": "trump",
#   "_id": "..."
#
# }
# ...

class ChannelStorySummary(object):
  def __init__(self):
    self.channels = {}

  def get(self, channel_id):
    return self.channels.get(channel_id, [])

  def put(self, channel_id, stories_summary):
    self.channels[channel_id] = stories_summary

channel_stories = ChannelStorySummary()
