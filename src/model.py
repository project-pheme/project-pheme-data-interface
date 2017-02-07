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
  __slots__ = [ '_id', 'display_name', 'description', 'updated' ]


class Story(BaseModel):
  __slots__ = [ '_id', 'channel_id', 'event_id', 'size',
    'start_date', 'last_activity', 'average_activity', 
    'controversiality', 'pub_count', 'img_count', 'verified_count',
    'title', 'most_shared_img' ]

  @staticmethod
  def idgen(channel_id, event_id):
    return "%s@%s" % (event_id, channel_id)

  def __init__(self, **kwargs):
    super(Story, self).__init__(**kwargs)
    self.update_calculated()

  def update_calculated(self):
    # Initialize calculated properties
    if (hasattr(self, 'channel_id') and self.channel_id is not None) and \
       (hasattr(self, 'event_id') and self.event_id is not None):
      object.__setattr__(self, '_id', self.idgen(self.channel_id, self.event_id))
    if (hasattr(self, 'size') and self.size is not None) and \
       (hasattr(self, 'start_date') and self.start_date is not None) and \
       (hasattr(self, 'last_activity') and self.last_activity is not None):
      duration = (self.last_activity - self.start_date).total_seconds()
      if duration == 0:
        avg = self.size
      else:
        avg = self.size / duration
      object.__setattr__(self, 'average_activity', avg)


class Entity(BaseModel):
  __slots__ = [ 'uri', 'name', 'type' ]


class Thread(BaseModel):
  __slots__ = [ 'uri', 'created_at', 'avg_rumour_coefficient', 'evidence', 'featured_tweet', 'verified_authors_present' ]

  @staticmethod
  def retweets_cleanup(threads):
    # Given an array of threads, return a new array where the retweets have
    # been eliminated, leaving only the oldest one
    import re
    from copy import copy
    #
    threads = sorted(copy(threads), key=lambda thread: thread.featured_tweet['date'])
    for i in range(0,len(threads)):
      # All the array has been consumed
      if i >= len(threads):
        break
      text_i = threads[i].featured_tweet['text']
      # Look for threads where the featured tweet has the same text or (RT.*<same text)
      popped_k = 0
      for j in range(i+1, len(threads)):
        j = j - popped_k   # compensate shrinking list
        if j >= len(threads):
          break
        text_j = threads[j].featured_tweet['text']
        # Pop from the list any exact text matches or matches with the RT prefix
        if text_i == text_j or re.match(r'RT\s+%s' % re.escape(text_i[:130]), text_j):
          threads.pop(j)
          popped_k = popped_k + 1
    #
    return threads


class Tweet(BaseModel):
  __slots__ = [ 'uri', 'created_at', 'rumour_coefficient', 'textual_content', 'veracity', 'veracity_score' ]


class User(BaseModel):
  __slots__ = [ 'uri', 'account_name', 'followers', 'friends' ]
