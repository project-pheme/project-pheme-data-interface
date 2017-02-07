#!/usr/bin/env python

from tornado.httpclient import HTTPRequest, AsyncHTTPClient
from tornado import gen
from json import loads
from rdflib.plugins.sparql.results.jsonresults import JSONResult
from datetime import datetime

from string import Template
import logging, urllib, pytz, os, re

import iso8601

GRAPHDB_ENDPOINT = os.environ["GRAPHDB_ENDPOINT"] if "GRAPHDB_ENDPOINT" in os.environ else 'http://pheme.ontotext.com/graphdb/repositories/pheme'

GRAPHDB_PHEME_VERSIONS=[ "v8" ]

logger = logging.getLogger('tornado.general')

_graphdb_pheme_versions = "(%s)" % ",".join(map(lambda v: "\"%s\"" % v, GRAPHDB_PHEME_VERSIONS))

def datetime_to_iso(dt, default=0):
  if dt is None:
      dt = datetime.utcfromtimestamp(default).replace(tzinfo=pytz.utc);
  if dt.tzinfo is None:
    raise Exception("datetime provided is tz-unaware!")
  return dt.isoformat()

def _avatar_process(url):
  if url is not None and url != "":
    return re.sub(r'_normal\.([a-z]{3,5})$', r'_bigger.\1', url)
  else:
    # default avatar (egg)
    return "https://abs.twimg.com/sticky/default_profile_images/default_profile_2_bigger.png"

def _str_to_bool(str):
  return (str or "").lower() in [ 'true', '1' ]

@gen.coroutine
def query(query):
  logger.info("Sending query:\n%s" % query)
  r = HTTPRequest(
    GRAPHDB_ENDPOINT,
    method='POST',
    headers={
      'Accept': 'application/sparql-results+json',
      'Content-Type': 'application/x-www-form-urlencoded'
      },
    body=urllib.urlencode({ 'query': query }),
    connect_timeout=30,
    request_timeout=300
    )
  http_client = AsyncHTTPClient()
  response = yield http_client.fetch(r)
  if response.error:
    raise Exception("Bad GraphDB response " + str(response))
  else:
    raise gen.Return(JSONResult(loads(response.body)))

import model

class Story(model.Story):   # aka Theme / Pheme
  @staticmethod
  @gen.coroutine
  def fetch_updated_since(channel, since=None, limit=100, order="ASC"):
    # Query graphdb for latest events belonging to the given channel
    q = Template("""
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      SELECT ?eventId (MAX(?date) AS ?lastUpdate)
      WHERE {   
          ?a pheme:createdAt ?date.
          FILTER (?date >= "$since_date"^^xsd:dateTime).
          ?a pheme:eventId ?eventId.
          FILTER (xsd:integer(?eventId) > -1).
          ?a pheme:dataChannel "$data_channel_id".
          ?a pheme:version ?pheme_version.
          FILTER ( ?pheme_version IN $pheme_versions ).
      } GROUP BY ?eventId
      ORDER BY $order(?lastUpdate)
      LIMIT $limit
    """).substitute(
      data_channel_id=channel._id,
      since_date=datetime_to_iso(since),
      pheme_versions=_graphdb_pheme_versions,
      order=order,
      limit=limit)
    result = yield query(q)
    result = filter(lambda x: x['eventId'] is not None, result)
    stories = map(lambda x:
                   Story(channel_id=channel._id,
                         event_id=x['eventId'].decode(),
                         last_activity=iso8601.parse_date(x['lastUpdate'].decode()),
                         ), result)
    raise gen.Return(stories)

  @gen.coroutine
  def get_latest_title(self):
    q = Template("""
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      SELECT ?phemeTitle 
      WHERE {
        ?a pheme:createdAt ?date.
        ?a pheme:eventId "$event_id" .
        ?a pheme:dataChannel "$data_channel_id".
        ?a pheme:version ?pheme_version.
        ?a pheme:eventClusterTitle ?phemeTitle.
        FILTER ( ?pheme_version IN $pheme_versions ).
      } 
      ORDER BY DESC(?date)
      LIMIT 1
    """).substitute(event_id=self.event_id, data_channel_id=self.channel_id, pheme_versions=_graphdb_pheme_versions)
    result = yield query(q)
    assert len(result) == 1

    x = iter(result).next()
    raise gen.Return(x['phemeTitle'].decode())

  @gen.coroutine
  def get_extended_metadata(self):
    q = Template("""
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      PREFIX sioc: <http://rdfs.org/sioc/ns#>

      select (sum(xsd:integer(xsd:boolean(?verified))) as ?verified_count) 
             (count(distinct ?imageURL) as ?img_count) 
             (count(distinct ?URL) as ?pub_count)
             (count(?a) as ?size)
             (MIN(?date) as ?start_date)
      where {   
        ?a a pheme:Tweet .
        ?a pheme:createdAt ?date.        
        ?a pheme:eventId "$event_id" .
        ?a pheme:version ?pheme_version.
        FILTER ( ?pheme_version IN $pheme_versions ).
        ?a pheme:dataChannel "$data_channel_id".
        OPTIONAL {?a pheme:hasEvidentialityPicture ?imageURL} .
        OPTIONAL {?a pheme:hasEvidentialityUrl ?URL} .
        ?a sioc:has_creator ?user .
        ?user pheme:twitterUserVerified ?verified .  
      }
    """).substitute(event_id=self.event_id, data_channel_id=self.channel_id, pheme_versions=_graphdb_pheme_versions)
    result = yield query(q)
    assert len(result) == 1

    x = iter(result).next()
    raise gen.Return(dict(
      size= int(x['size'].decode()),
      start_date= iso8601.parse_date(x['start_date'].decode()),
      verified_count= int(x['verified_count'].decode()),
      img_count= int(x['img_count'].decode()),
      pub_count = int(x['pub_count'].decode())
    ))

  @gen.coroutine
  def get_featured_tweet(self):
    # Grab featured tweet in Theme
    # (currently, the oldest)
    q = Template("""
      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      PREFIX dlpo: <http://www.semanticdesktop.org/ontologies/2011/10/05/dlpo#>
      PREFIX sioc: <http://rdfs.org/sioc/ns#>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

      select ?thread ?source ?text ?userName ?userHandle ?verified ?date (count(?a) as ?countReplies) ?avatar where {   
        ?a a pheme:ReplyingTweet .
        ?source a pheme:SourceTweet.
        ?source sioc:has_container ?thread.
        ?source sioc:has_creator ?creator.
        ?creator foaf:name ?userName.
        ?creator foaf:accountName ?userHandle.
        OPTIONAL { ?creator foaf:depiction ?avatar. }
        ?creator pheme:twitterFollowersCount ?numberOfFollowers .
        ?creator pheme:twitterUserVerified ?verified .
        ?source pheme:createdAt ?date.
        ?source dlpo:textualContent ?text.
        ?a sioc:has_container ?thread.
        ?a pheme:eventId "$event_id".
        ?a pheme:dataChannel "$data_channel_id".
        ?a pheme:version ?pheme_version.
        FILTER ( ?pheme_version IN $pheme_versions ).
      } GROUP BY ?thread ?source ?text ?userName ?userHandle ?verified ?date ?avatar
      order by desc(?countReplies)
      limit 1
    """).substitute(event_id=self.event_id, data_channel_id=self.channel_id, pheme_versions=_graphdb_pheme_versions)
    result = yield query(q)
    assert len(result) == 1   # Because of grouping, there must always be a result row

    # If there were no real results from the query, try an alternative one
    x = iter(result).next()
    if x['text'] is None:
      logger.info("No replies / retweets in cluster, using alternative query")
      x = yield self._get_featured_tweet_alt()
    # Decode source to tweet id
    tweet_id = re.match(r'.*\D(\d+)$', x['source'].decode())
    if tweet_id is None:
      raise Exception("Unparseable tweet_id from result %s" % str(x))
    else:
      tweet_id = tweet_id.groups()[0]
    # Use results
    if x is not None and x['text'] is not None:
      logger.info("Representative tweet: " + str(x))
      raise gen.Return(dict(
        tweet_id= tweet_id,
        text= unicode(x['text']),
        date= iso8601.parse_date(x['date'].decode()),
        user= dict(
          profile_image_url = _avatar_process(x['avatar']),
          user_description= x['userName'],
          user_screen_name= x['userHandle']),
          is_verified = (x['verified'].capitalize() == 'True')
        ))

  @gen.coroutine
  def get_linked_images(self):
    q = Template("""
      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      PREFIX dlpo: <http://www.semanticdesktop.org/ontologies/2011/10/05/dlpo#>
      PREFIX sioc: <http://rdfs.org/sioc/ns#>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>

      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      select (MIN(?cDate) as ?date) ?imageURL (count(?imageURL) as ?countImage) where {   
          ?a pheme:createdAt ?cDate .
          ?a pheme:hasEvidentialityPicture ?imageURL .
          ?a pheme:eventId "$event_id".
          ?a pheme:dataChannel "$data_channel_id".
          ?a pheme:version ?pheme_version.
          FILTER ( ?pheme_version IN $pheme_versions ).
      } group by ?imageURL
      having (?countImage > 0)
    """).substitute(event_id=self.event_id, data_channel_id=self.channel_id, pheme_versions=_graphdb_pheme_versions)
    result = yield query(q)

    raise gen.Return(map(lambda x: dict(
                          date= iso8601.parse_date(x['date'].decode()),
                          imgUrl= x['imageURL'].decode(),
                          count= int(x['countImage'].decode())),
                         result))

  @gen.coroutine
  def get_related_articles(self):
    q = Template("""
      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      PREFIX dlpo: <http://www.semanticdesktop.org/ontologies/2011/10/05/dlpo#>
      PREFIX sioc: <http://rdfs.org/sioc/ns#>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>

      select ?date ?text ?thread ?URL where {   
          ?a pheme:createdAt ?date.
          ?a sioc:has_container ?thread .
          ?a pheme:hasEvidentialityUrl ?URL .
          ?a dlpo:textualContent ?text.
          ?a pheme:eventId "$event_id".
          ?a pheme:dataChannel "$data_channel_id".
          ?a pheme:version ?pheme_version.
          FILTER ( ?pheme_version IN $pheme_versions ).
      } order by desc(?date)
    """).substitute(event_id=self.event_id, data_channel_id=self.channel_id, pheme_versions=_graphdb_pheme_versions)
    result = yield query(q)

    articles = map(lambda x: dict(
                          date= iso8601.parse_date(x['date'].decode()),
                          text= unicode(x['text']),
                          thread= x['thread'].decode(),
                          url= x['URL'].decode()),
                         result)

    from url_utils import get_canonical_url
    from urlparse import urlparse
    for art in articles:
      canonical_url = yield get_canonical_url(art['url'])
      canonical_url_p = urlparse(canonical_url)
      art['canonicalUrl'] = {
        "url": canonical_url,
        "scheme": canonical_url_p.scheme,
        "netloc": canonical_url_p.netloc,
        "path": canonical_url_p.path
      }

    raise gen.Return(articles)

  @gen.coroutine
  def get_author_locations(self):
    q = Template("""
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      PREFIX pub: <http://ontology.ontotext.com/taxonomy/>
      PREFIX wgs84_pos: <http://www.w3.org/2003/01/geo/wgs84_pos#>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
      PREFIX sioc: <http://rdfs.org/sioc/ns#>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      PREFIX dlpo: <http://www.semanticdesktop.org/ontologies/2011/10/05/dlpo>

      select ?userHandle ?date ?lat ?long ?text {
          ?t pheme:version ?pheme_version.
          FILTER ( ?pheme_version IN $pheme_versions ).
          ?t pheme:createdAt ?date .
          ?t pheme:userLocations ?loc.
          ?t pheme:dataChannel "$data_channel_id".
          ?t pheme:eventId "$event_id" .
          ?t dlpo:\#textualContent ?text.
          ?loc pheme:inst ?location.
          ?location a pub:Location.
          ?location pub:exactMatch ?geo.
          ?geo wgs84_pos:long ?long.
          ?geo wgs84_pos:lat ?lat.
          ?t sioc:has_creator ?u.
          ?u foaf:name ?userName.
          ?u foaf:accountName ?userHandle.
      }
      LIMIT 100
    """).substitute(event_id=self.event_id, data_channel_id=self.channel_id, pheme_versions=_graphdb_pheme_versions)
    result = yield query(q)

    locations = map(lambda x: dict(
                            userHandle= unicode(x['userHandle']),
                            date= iso8601.parse_date(x['date'].decode()),
                            lat= float(x['lat']),
                            long= float(x['long']),
                            text= unicode(x['text'])),
                          result)

    raise gen.Return(locations)

  @gen.coroutine
  def _get_featured_tweet_alt(self):
    # Just retrieve the oldest tweet from an event
    q = Template("""
      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      PREFIX dlpo: <http://www.semanticdesktop.org/ontologies/2011/10/05/dlpo#>
      PREFIX sioc: <http://rdfs.org/sioc/ns#>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>

      select (?a AS ?source) ?text ?date ?userName ?userHandle ?avatar ?verified
      where {
        ?a pheme:eventId "$event_id".
        ?a pheme:dataChannel "$data_channel_id".
        ?a pheme:createdAt ?date.
        ?a dlpo:textualContent ?text.
        ?a sioc:has_creator ?u.
        ?u foaf:name ?userName.
        ?u foaf:accountName ?userHandle.
        OPTIONAL { ?u foaf:depiction ?avatar. }
        ?u pheme:twitterFollowersCount ?numberOfFollowers .
        ?u pheme:twitterUserVerified ?verified .
        ?a pheme:version ?pheme_version.
        FILTER ( ?pheme_version IN $pheme_versions ).
      }
      order by DESC(?verified) DESC(?numberOfFollowers) ?date
      limit 1
    """).substitute(event_id=self.event_id, data_channel_id=self.channel_id, pheme_versions=_graphdb_pheme_versions)
    result = yield query(q)
    if len(result) == 1:
      tweet = iter(result).next()
      raise gen.Return(tweet)

  @gen.coroutine
  def get_controversiality_score(self):
    #
    q = Template("""
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

      select ?sdq_type (count(?sdq_type) as ?count) where {
        ?a a pheme:Tweet .
        ?a pheme:eventId "$event_id".
        ?a pheme:dataChannel "$data_channel_id".
        ?a pheme:sdq ?sdq_type .
        ?a pheme:version ?pheme_version.
        FILTER ( ?pheme_version IN $pheme_versions ).
      } group by ?sdq_type
    """).substitute(event_id=self.event_id, data_channel_id=self.channel_id, pheme_versions=_graphdb_pheme_versions)
    result = yield query(q)

    # v will hold the count for each sdq_type
    v = dict(deny=0.0, support=0.0, question=0.0)
    for x in result:
      sdq_type = x['sdq_type'].decode()
      sdq_count = int(x['count'].decode())
      v[sdq_type] = float(sdq_count)
    # c holds the sum of the counts
    c = reduce(lambda c,k: c + v[k], v.keys(), 0.0)
    # avoid division by 0
    if c == 0.0:
      raise gen.Return(0.0)
    else:
      score = (1.0/3.0) * (
                pow(v['support'] / c - (1.0/3.0), 2) +
                pow(v['deny'] / c - (1.0/3.0), 2) +
                pow(v['question'] / c - (1.0/3.0), 2)
              )
      raise gen.Return(1.0 - (9.0/2.0) * score)

  @gen.coroutine
  def get_author_geolocations(self):
    # Articles referenced from the Story (cluster) tweets
    raise Exception("not implemented")

  @gen.coroutine
  def get_referenced_geolocations(self):
    # Articles referenced from the Story (cluster) tweets
    raise Exception("not implemented")


_lipsum = [
  "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam vitae ligula aliquam lectus lobortis porta non a diam. Proin fringilla augue.",
  "Duis ultrices vestibulum lacus non luctus. Praesent aliquet, justo ut accumsan bibendum, sem erat sagittis nunc, a vulputate diam ulla nisi.",
  "Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit",
  "At vero eos et accusamus et iusto odio dignissimos ducimus qui blanditiis praesentium voluptatum deleniti atque corrupti" ]

class Thread(model.Thread):
  # @staticmethod
  # @gen.coroutine
  # def fetch_from_story(story):
  #   ### TODO: just sample data by now ###
  #   from random import randint, sample
  #   from datetime import datetime, timedelta
  #   from pytz import UTC
  #   results = []
  #   for x in range(0, randint(3,10)):
  #     featured_tweet = dict(
  #       text= sample(_lipsum, 1)[0],
  #       date= datetime(2016,1,1,tzinfo=pytz.UTC) + timedelta(seconds=randint(0,(365*24*3600)/2)),
  #       user= dict(
  #         profile_image_url = 'https://lh6.ggpht.com/Gg2BA4RXi96iE6Zi_hJdloQAZxO6lC6Drpdr7ouKAdCbEcE_Px-1o4r8bg8ku_xzyF4y=h900',
  #         user_description= 'Twitter User',
  #         user_screen_name= 'twitteruser')
  #       )
  #     t = Thread(uri= "random%d" % x, featured_tweet=featured_tweet)
  #     results.append(t)
  #   raise gen.Return(results)

  @staticmethod
  @gen.coroutine
  def fetch_from_story(story):
    q = Template("""
      PREFIX sioc: <http://rdfs.org/sioc/ns#>
      PREFIX pheme: <http://www.pheme.eu/ontology/pheme#>
      PREFIX dlpo: <http://www.semanticdesktop.org/ontologies/2011/10/05/dlpo#>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

      select *
        {
          {
          select ?thread (min(?date) as ?first) (sum(xsd:integer(xsd:boolean(?verified))) as ?countVerifiedUsers)
            {
            ?tweet a pheme:Tweet .
            ?tweet pheme:version ?pheme_version.
            FILTER ( ?pheme_version IN $pheme_versions ).
            ?tweet sioc:has_container ?thread .
            ?tweet pheme:dataChannel "$data_channel_id" .
            ?tweet pheme:createdAt ?date .
            ?tweet sioc:has_creator ?user .
            ?user pheme:twitterUserVerified ?verified .
            ?tweet pheme:eventId "$event_id" .
            } group by ?thread 
          }
        ?tweet sioc:has_container ?thread .
        ?tweet pheme:createdAt ?first .
        ?tweet dlpo:textualContent ?text .
        ?tweet sioc:has_creator ?user .
        OPTIONAL { ?tweet pheme:veracity ?veracity . }
        OPTIONAL { ?tweet pheme:veracityScore ?veracity_score . }
        ?user foaf:accountName ?accountName .
        ?user foaf:name ?userName .
        ?user pheme:twitterFollowersCount ?numberOfFollowers .
        ?user pheme:twitterStatusesCount ?numberOfPosts .
        OPTIONAL { ?user foaf:depiction ?avatar . }
        ?user pheme:twitterUserVerified ?verified .
        }
        order by DESC(?verified) DESC(?numberOfFollowers)
        limit 100
    """).substitute(event_id=story['event_id'], data_channel_id=story['channel_id'], pheme_versions=_graphdb_pheme_versions)
    result = yield query(q)

    results = []
    for x in result:
      tweet_id = re.match(r'.*\D(\d+)$', x['thread'].decode())
      if tweet_id is None:
        raise Exception("Unparseable tweet_id from result %s" % str(x))
      else:
        tweet_id = tweet_id.groups()[0]
      featured_tweet = dict(
        tweet_id= tweet_id,
        text= unicode(x['text']),
        date= iso8601.parse_date(x['first'].decode()),
        veracity= _str_to_bool(x['veracity']),
        veracity_score= x['veracity_score'] or 0.0,
        user= dict(
          profile_image_url = x['avatar'],
          user_description = x['userName'],
          user_screen_name = x['accountName'],
          is_verified = (x['verified'].capitalize() == 'True')
          )
        )
      #
      tweet_id = re.match(r'.*\D(\d+)$', x['thread'].decode())
      if tweet_id is None:
        raise Exception("Unparseable tweet_id from result %s" % str(x))
      else:
        tweet_id = tweet_id.groups()[0]
      uri = Template("https://twitter.com/$user_screen_name/status/$tweet_id").substitute(
        user_screen_name= x['accountName'],
        tweet_id= tweet_id
        )
      #
      t = Thread(uri= uri, featured_tweet= featured_tweet)
      results.append(t)
      
    raise gen.Return(results)

