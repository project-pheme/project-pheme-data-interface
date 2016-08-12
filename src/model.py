#!/usr/bin/env python

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
        if hasattr(self, k):
          ret[k] = self.__getattribute__(k)
      except AttributeError, e:
        print "error on %s" % k
        print e
        pass
    return ret


class Channel(BaseModel):
  __slots__ = [ '_id', 'display_name', 'description' ]


class Story(BaseModel):
  __slots__ = [ 'channel_id', 'event_id', 'size',
    'start_date', 'last_activity', 'average_activity', 
    'controversiality', 'pub_count', 'img_count', 'verified_count' ]

  def __init__(self, **kwargs):
    super(Story, self).__init__(**kwargs)
    self.update_calculated()

  def update_calculated(self):
    # Initialize calculated properties
    if (hasattr(self, 'size') and self.size is not None) and \
       (hasattr(self, 'start_date') and self.start_date is not None) and \
       (hasattr(self, 'last_activity') and self.last_activity is not None):
      duration = (self.last_activity - self.start_date).total_seconds() / 3600.0
      if duration == 0:
        avg = self.size
      else:
        avg = self.size / ((self.last_activity - self.start_date).total_seconds() / 3600.0)
      object.__setattr__(self, 'average_activity', avg)


class Entity(BaseModel):
  __slots__ = [ 'uri', 'name', 'type' ]


class Thread(BaseModel):
  __slots__ = [ 'uri', 'created_at', 'avg_rumour_coefficient', 'evidence', 'featured_tweet', 'verified_authors_present' ]


class Tweet(BaseModel):
  __slots__ = [ 'uri', 'created_at', 'rumour_coefficient', 'textual_content', ]


class User(BaseModel):
  __slots__ = [ 'uri', 'account_name', 'followers', 'friends' ]
