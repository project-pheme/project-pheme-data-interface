from tornado import gen

from kafka_topic_consumer import KafkaTopicConsumer

class PhemeEventConsumer(object):
  def __init__(self, topic_name, saved_search):
    self.saved_search = saved_search
    self.kafka_consumer = KafkaTopicConsumer(
      kafka_hosts="localhost:9092",
      topic_name=topic_name
    )

  @gen.coroutine
  def consume_one(self):
    event = yield self.kafka_consumer.get()
    if 'event_cluster' in event:
      event_id = event['event_cluster']
      # increase traffic counter of the event
      yield self.saved_search.count_event_in_ranking(event_id)
      #
      # expand and normalise
      normalised_event = self._process_event(event)
      for tweet in normalised_event:
       yield self.saved_search[event_id].add(tweet)

  def _process_event(self, event):
    # Get the entities discovered by the entity analyzer
    pheme_entities = None
    if 'pheme_entities' in event and 'pheme_entity_texts' in event:
      pheme_entities =\
        map(lambda x: { "indices": x[0][0:1], "text": x[1], "kind": x[0][2] },
            zip(event['pheme_entities'], event['pheme_entity_texts']))
      event['entities']['pheme'] = pheme_entities

    # Create '_users' property array
    event['_users'] = [ event['user'] ]

    normalised_event = [ event ]

    # if there's a retweeted tweet
    if 'retweeted_status' in event:
      retweeted = event['retweeted_status']
      retweeted['event_cluster'] = event['event_cluster']
      if pheme_entities is not None:
        retweeted['entities']['pheme'] = pheme_entities
      #
      event['_users'].insert(0, retweeted['user'])
      #
      normalised_event.insert(0, retweeted)

    # if there's a quoted tweet
    if 'quoted_status' in event:
      quoted = event['quoted_status']
      quoted['event_cluster'] = quoted['event_cluster']
      #
      event['_users'].insert(0, retweeted['user'])
      #
      normalised_event.insert(0, quoted)

    return normalised_event

  @gen.coroutine
  def consume(self):
    while True:
      yield self.consume_one()

  def run(self, io_loop):
    io_loop.spawn_callback(self.consume)

  def close(self):
    self.kafka_consumer.close()
