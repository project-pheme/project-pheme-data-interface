from tornado.httpclient import HTTPRequest, AsyncHTTPClient
from tornado import gen
from my_json import loads, dumps
from string import Template
from copy import copy

import logging, time, re, urllib, pytz, os
from datetime import datetime

import model
from tasks import SelfRegulatingTask, register_task

logger = logging.getLogger('tornado.general')

platform_proto = os.environ["PLATFORM_PROTO"] if "PLATFORM_PROTO" in os.environ else "http"
platform_host = os.environ["PLATFORM_HOST"] if "PLATFORM_HOST" in os.environ else "localhost"
platform_port = os.environ["PLATFORM_PORT"] if "PLATFORM_PORT" in os.environ else "8080"
ush_username = os.environ["USH_USERNAME"] if "USH_USERNAME" in os.environ else "admin"
ush_password = os.environ["USH_PASSWORD"] if "USH_PASSWORD" in os.environ else "admin"

USH_CLIENT_ID="ushahidiui"
USH_CLIENT_SECRET="35e7f0bca957836d05ca0492211b0ac707671261"

USH_BASEURL=platform_proto + "://" + platform_host + ":" + platform_port

required_post_types = {
  "Themes": {
    "stages": [{
      "label": "Structure",
      "fields": [ 
        { "label": "Theme ID",         "type": "varchar",  "input": "text",   "required": True, "key": "theme-id" },
        { "label": "Channel ID",       "type": "varchar",  "input": "text",   "required": True, "key": "theme-channel-id" },
        { "label": "Size",             "type": "int",      "input": "number", "required": True, "key": "theme-size" },
        { "label": "Start date",       "type": "datetime", "input": "date",   "required": True, "key": "theme-start-date" },
        { "label": "Last activity",    "type": "datetime", "input": "date",   "required": True, "key": "theme-last-activity" },
        { "label": "Average activity", "type": "decimal",  "input": "number", "required": True, "key": "theme-average-activity" },
        { "label": "Featured tweet",   "type": "text",     "input": "textarea", "required": True, "key": "theme-featured-tweet" },
      ]
    }]
  }
}

v3_link = None
def get_link():
  # Singleton wrapper  
  global v3_link
  if v3_link is None:
      v3_link = UshahidiV3Link()
  return v3_link

def datetime_to_timestamp(dt):
  return int(time.mktime(dt.utctimetuple()))

def datetime_to_string(dt):
  return dt.strftime("%Y-%m-%d %H:%M:%S")

def string_to_datetime(str):
  return datetime.strptime(str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)

class Channel(model.Channel):
  @staticmethod
  def as_copy(channel):
    n = Channel()
    for k in model.Channel.__slots__:
      if hasattr(channel, k):
        object.__setattr__(self, k, getattr(channel, k))
    self.set_category_id(None)

  @staticmethod
  def create_from_v3_tag(tag):
    n = Channel(_id=Channel.decode_slug(tag["slug"]), display_name=tag["tag"], description=tag["description"])
    n.set_category_id(tag["id"])
    return n

  @staticmethod
  def create_from_datachannel(dc):
    n = Channel(_id=dc["channelID"], display_name=dc["name"], description=dc["description"])
    n.set_category_id(None)
    return n

  @staticmethod
  @gen.coroutine
  def get_channels():
    # Get data channels (created as categories)
    results = {}
    channels = yield get_link().do_request("/api/v3/tags", method='GET')
    if 'results' in channels:
      for tag in channels['results']:
        channel = Channel.create_from_v3_tag(tag)
        results[channel._id] = channel
    raise gen.Return(results)

  def set_category_id(self, the_id):
    object.__setattr__(self, 'category_id', the_id)

  @staticmethod
  def decode_slug(slug):
    match = re.match("pheme-data-channel-(\w+)", slug)
    if match:
      return match.groups()[0]

  def encode_slug(self):
    return "pheme-data-channel-%s" % self._id

  @gen.coroutine
  def _create(self):
    # Create a v3 category associated to this data channel
    tag_json = {
      "type": "category",
      "tag": self.display_name,
      "description": self.description,
      "slug": self.encode_slug(),
      "icon": "tag"
    }
    tag = yield get_link().do_request("/api/v3/tags", method='POST', body=dumps(tag_json))
    self.set_category_id(tag["id"])

  def obj(self):
    obj = super(Channel, self).obj()
    obj['category_id'] = self.category_id
    return obj

class Story(model.Story):
  def __init__(self, **kwargs):
    super(Story, self).__init__(**kwargs)
    featured_tweet = kwargs['featured_tweet'] if 'featured_tweet' in kwargs else None
    object.__setattr__(self, 'featured_tweet', featured_tweet)

  @staticmethod
  def as_copy(story, **kwargs):
    n = Story(**kwargs)
    for k in model.Story.__slots__:
      if hasattr(story, k):
        object.__setattr__(n, k, getattr(story, k))
    return n

  @staticmethod
  @gen.coroutine
  def find_by_post_id(post_id):
    post = yield get_link().do_request("/api/v3/posts/%s" % str(post_id), method='GET')
    if 'errors' in post:
      raise gen.Return(None)
    else:
      story = Story(
        event_id= post["values"]["theme-id"][0],
        channel_id= post["values"]["theme-channel-id"][0],
        size= post["values"]["theme-size"][0],
        start_date= string_to_datetime(post["values"]["theme-start-date"][0]),
        last_activity= string_to_datetime(post["values"]["theme-last-activity"][0]),
        featured_tweet= loads(post["values"]["theme-featured-tweet"][0])
        )
      raise gen.Return(story)

  @staticmethod
  @gen.coroutine
  def find_by_id(event_id):
    form_id = get_link().post_types['Themes']['id']
    lookup_qs = dict(form=[ form_id ], values={ "theme-id": event_id }, limit=1, offset=0, status="all")
    lookup_res = yield get_link().do_request("/api/v3/posts", qs=lookup_qs, method='GET')
    if 'results' not in lookup_res or len(lookup_res['results']) == 0:
      raise gen.Return(None)
    else:
      post = lookup_res['results'][0]
      story = Story(
        event_id= post["values"]["theme-id"][0],
        channel_id= post["values"]["theme-channel-id"][0],
        size= post["values"]["theme-size"][0],
        start_date= string_to_datetime(post["values"]["theme-start-date"][0]),
        last_activity= string_to_datetime(post["values"]["theme-last-activity"][0]),
        featured_tweet= loads(post["values"]["theme-featured-tweet"][0])
        )
      raise gen.Return(story)

  @gen.coroutine
  def save(self):
    if not hasattr(self, 'event_id') or self.event_id is None:
      logging.info("Refusing to save story without event_id")
      return
    # save / update based on story/event id
    existing_story = yield Story.find_by_id(self.event_id)
    if not existing_story:
      # create new one
      yield self._create()
    else:
      # update existing one
      logging.error("ACHTUNG!: not supporting updating v3 (yet)")

  def _fill_v3_obj(self, post):
    # Fills v3 object with values from this object
    if "values" not in post: post["values"] = {}
    post["values"]["theme-id"] = [ int(self.event_id) ]
    post["values"]["theme-channel-id"] = [ self.channel_id ]
    post["values"]["theme-size"] = [ self.size ]
    post["values"]["theme-start-date"] = [ datetime_to_string(self.start_date) ]
    post["values"]["theme-last-activity" ] = [ datetime_to_string(self.last_activity) ]
    post["values"]["theme-average-activity"] = [ "%0.2f" % self.average_activity ]
    post["values"]["theme-featured-tweet"] = [ dumps(self.featured_tweet) ]
    # Add some other derived metadata if not present
    if "created" not in post: post["created"] = datetime_to_timestamp(self.start_date)
    if "title" not in post: post["title"] = "ID %s" % str(self.event_id)
    if "content" not in post: post["content"] = "Theme ID %s" % str(self.event_id)
    if "slug" not in post: post["slug"] = "theme-id-%s" % str(self.event_id)
    # Check it's added to the relevant category (aka event, aka channel)
    category_id = get_link().events[self.channel_id].category_id
    if category_id not in post["tags"]:
      post["tags"].append(category_id)

  @gen.coroutine
  def _create(self):
    form = get_link().post_types['Themes']
    post_json = loads("""{
      "locale": "en_US",
      "type": "report",
      "status": "draft",
      "values": { },
      "completed_stages": [],
      "form": {},
      "tags": [],
      "allowed_privileges": [ "read", "create", "update", "delete", "search", "change_status" ]
    }""")
    post_json["form"] = form
    self._fill_v3_obj(post_json)
    #
    logger.info("Creating the following post: " + dumps(post_json))
    response = yield get_link().do_request(
      "/api/v3/posts",
      method='POST',
      body=dumps(post_json),
    )

class TokenRefreshTask(SelfRegulatingTask):
  def __init__(self, expires_in):
    super(TokenRefreshTask, self).__init__(task_id="ush_v3_token_refresh", first_delay=max(expires_in * 2 / 3, 60))

  @gen.coroutine
  def workload(self):
    import time
    logger.info("Refreshing V3 access token")
    expires_in = yield get_link().get_new_token()
    self.next_exec = time.time() + max(expires_in * 2 / 3, 60)

class UshahidiV3Link(object):
    def __init__(self):
      self.access_token = None
      self.expires = None
      self.post_type_ids = {}
      self.post_types = {}
      self.events = {}    # === data channels
      self.channels = {}

    @gen.coroutine
    def initialise_tasks(self):
      # Ush V3 access token
      expires_in = yield self.get_new_token()
      logger.info("Starting V3 access token refresh task")
      t = TokenRefreshTask(expires_in)
      register_task(t, start=True)

      # Ensure V3 post types
      logger.info("Ensuring post types presence in V3 instance")
      yield v3_link.ensure_post_types()

      # Load V3 defined events (channels)
      logger.info("Loading events defined in V3 instance")
      yield v3_link.load_defined_events()

    def build_token_request(self):
      return dict(
          client_id= USH_CLIENT_ID,
          client_secret= USH_CLIENT_SECRET,
          grant_type= "password",
          username= ush_username,
          password= ush_password,
          scope= "posts media forms api tags savedsearches sets users stats layers config messages notifications contacts roles permissions csv dataproviders",
      )

    @gen.coroutine
    def get_new_token(self):
      r = HTTPRequest(
        USH_BASEURL + "/oauth/token",
        method='POST',
        headers={
          'Accept': 'application/json',
          'Content-Type': 'application/json'
          },
        body=dumps(self.build_token_request()),
      )
      http_client = AsyncHTTPClient()
      response = yield http_client.fetch(r)
      if response.error:
          raise Exception("Bad Oauth token response " + str(response))
      else:
          response = loads(response.body)
          if not ('access_token' in response and 'expires' in response):
              raise Exception("Invalid Oauth token response")
          self.access_token = response['access_token']
          self.expires = response['expires']
          raise gen.Return(response['expires_in'])

    @gen.coroutine
    def do_request(self, endpoint, **kwargs):
      if not 'headers' in kwargs:
        kwargs['headers'] = {}
      
      if kwargs['method'] in [ 'POST', 'PUT' ]:
        kwargs['headers']['Content-Type'] = 'application/json'

      query_string = ""
      if 'qs' in kwargs:
        query_string = "?" + _v3_query_string(kwargs['qs'])
        del kwargs['qs']

      kwargs['headers']['Accept'] = 'application/json'
      kwargs['headers']['Authorization'] = 'Bearer %s' % self.access_token

      r = HTTPRequest(USH_BASEURL + endpoint + query_string, **kwargs)
      http_client = AsyncHTTPClient()
      response = yield http_client.fetch(r)
      if response.error:
        raise Exception("Problem doing request to Ushahidi v3 instance")
      else:
        raise gen.Return(loads(response.body))

    @gen.coroutine
    def load_defined_events(self):
      self.events = yield Channel.get_channels()
      ## TODO: kick off tasks for data import !

    @gen.coroutine
    def add_event(self, channel):
      if channel._id in self.events:
        raise Exception("Data channel id '%s' is already present in the dashboard" % channel._id)
      else:
        yield channel._create()
        self.events[channel._id] = channel
        self.channels[channel.category_id] = channel

    @gen.coroutine
    def ensure_post_types(self):
      # ensure presence of post types needed by this interface
      post_types = yield self.get_post_types()
      post_types_summary = dict(map(lambda pt: (pt['name'], { "id": pt['id'] } ), post_types))
      for pt in required_post_types:
        if not pt in post_types_summary:
          logging.info("Creating post type %s" % pt)
          post_type = yield self.create_post_type(pt)
        else:
          post_type = post_types_summary[pt]
        # save for future reference
        self.post_types[pt] = post_type
        self.post_type_ids[pt] = post_type["id"]

    @gen.coroutine
    def create_post_type(self, name):
      # Create the post type
      post_type = yield self.do_request("/api/v3/forms", method='POST', body=dumps({"name": name}))
      
      # Create the stages and attributes
      post_type['attributes'] = []
      post_type['grouped_attributes'] = []
      post_type['stages'] = []
      #
      for stage in required_post_types[name]['stages']:
        stage_spec = { "label": stage["label"], "required": "0", "formId": post_type["id"] }
        stage_obj = yield self.do_request(
          "/api/v3/forms/%d/stages" % post_type["id"],
          method= 'POST',
          body= dumps(stage_spec))
        post_type['stages'].append(stage_obj)
        ##
        attrib_idx = 0
        for attribute in stage['fields']:
          attrib_spec = copy(attribute)
          attrib_spec["formId"] = post_type["id"]
          attrib_spec["form_stage_id"] = stage_obj["id"]
          attrib_spec["priority"] = attrib_idx
          attrib_obj = yield self.do_request(
            "/api/v3/forms/%d/attributes" % post_type["id"],
            method= 'POST',
            body= dumps(attrib_spec))
          post_type['attributes'].append(attrib_obj)
          post_type['grouped_attributes'].append(attrib_obj)
          #
          attrib_idx = attrib_idx + 1
      #
      post_type = yield self.do_request(
        "/api/v3/forms/%d" % post_type['id'],
        method= 'PUT',
        body= dumps(post_type))
      raise gen.Return(post_type)
      
    @gen.coroutine
    def get_post_types(self):
      response = yield self.do_request("/api/v3/forms", method='GET')
      raise gen.Return(response["results"])

def _v3_query_string(params):
  url_params = []
  for (k,v) in params.iteritems():
    if type(v) in [list, tuple]:
      for v1 in v:
        if type(v1) not in [list, tuple, dict]:
          url_params.append((k+'[]', v1))
        else:
          raise Exception("Array of non-scalar is not supported")
    elif type(v) == dict:
      for (k1, v1) in v.iteritems():
        if type(v1) not in [list, tuple, dict]:
          url_params.append(('%s[%s]' % (k,k1), v1))
        else:
          raise Excception("Array of non-scalar is not supported")
    else:
      url_params.append((k,v))
  return urllib.urlencode(url_params)
