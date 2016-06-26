from datetime import datetime
import json

class myJSONEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, datetime):
      return obj.isoformat()
    elif hasattr(obj, 'obj'):
      return obj.obj()
    elif type(obj) == type:
      return str(type(obj))
    else:
      raise TypeError("Type %s not serializable" % str(type(obj)))

def dumps(*vargs, **kwargs):
  kwargs['cls']=myJSONEncoder
  return json.dumps(*vargs, **kwargs)

def loads(*vargs, **kwargs):
  return json.loads(*vargs, **kwargs)
