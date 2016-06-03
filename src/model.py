#!/usr/bin/env python

_fixed_channels = {
  'trump': {'_id': 'trump', 'topic_name': 'pheme_en_graphdb', 'display_name': 'Trump'}
}

from tornado import gen

class ReadOnlyError(Exception):
  pass


class BaseModel(object):
  def __init__(self, **kwargs):
    for k in self.__class__.__slots__:
      if k in kwargs:
        object.__setattr__(self, k, kwargs[k])

  def __setattr__(self, key, value):
    raise ReadOnlyError("Can't modify %s instances!" % self.__class__)

  def obj(self):
    ret = dict()
    for k in self.__class__.__slots__:
      try:
        ret[k] = self.__getattribute__(k)
      except AttributeError, e:
        print "error on %s" % k
        print e
        pass
    return ret


class Channel(BaseModel):
  __slots__ = [ '_id', 'topic_name', 'display_name' ]

  @staticmethod 
  @gen.coroutine 
  def get(the_id): 
    result = Channel(**(_fixed_channels[the_id])) 
    raise gen.Return(result) 

  @staticmethod 
  @gen.coroutine 
  def list(): 
    raise gen.Return(map(lambda c: Channel(**c), _fixed_channels.values()))

class Story(BaseModel):
  __slots__ = [ 'channel_id', 'event_id', 'size', 'start_date', 'last_activity', 'average_activity' ]

  def __setattr__(self):
    raise ReadOnlyError("Can't modify Channel instances!")

  def __init__(self, **kwargs):
    super(Story, self).__init__(**kwargs)

    # Initialize calculated properties
    if self.size is not None and self.start_date is not None and self.last_activity is not None:
      duration = (self.last_activity - self.start_date).total_seconds() / 3600.0
      if duration == 0:
        avg = self.size
      else:
        avg = self.size / ((self.last_activity - self.start_date).total_seconds() / 3600.0)
      object.__setattr__(self, 'average_activity', avg)


class Entity(BaseModel):
  __slots__ = [ 'uri', 'name', 'type' ]


class Thread(BaseModel):
  __slots__ = [ 'uri', 'created_at', 'avg_rumour_coefficient', 'evidence' ]

  def __setattr__(self):
    raise ReadOnlyError("Can't modify Thread instances!")


class Tweet(BaseModel):
  __slots__ = [ 'uri', 'created_at', 'rumour_coefficient', 'textual_content', ]

  def __setattr__(self):
    raise ReadOnlyError("Can't modify Tweet instances!")


class User(BaseModel):
  __slots__ = [ 'uri', 'account_name', 'followers', 'friends' ]

  def __setattr__(self):
    raise ReadOnlyError("Can't modify User instances!")