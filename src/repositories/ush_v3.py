from tornado.httpclient import HTTPRequest, AsyncHTTPClient
from tornado import gen
from json import loads, dumps
from string import Template

import logging, time


USH_CLIENT_ID="ushahidiui"
USH_CLIENT_SECRET="35e7f0bca957836d05ca0492211b0ac707671261"
USH_USERNAME="admin"
USH_PASSWORD="admin"

USH_BASEURL="http://b2d:8080"

v3_link = None
def get_link():
    global v3_link
    if v3_link is None:
        v3_link = UshahidiV3Link()
    return v3_link

class UshahidiV3Link(object):
    def init(self):
        self.access_token = None
        self.expires = None
    
    def build_token_request(self):
        return dict(
            client_id= USH_CLIENT_ID,
            client_secret= USH_CLIENT_SECRET,
            grant_type= "password",
            username= USH_USERNAME,
            password= USH_PASSWORD,
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
    def create_thread_as_post(self, thread, first_tweet):
        post_json = loads("""{
          "title": "$title",
          "content": "",
          "locale": "en_US",
          "status": "draft",
          "created": "",
          "values": {
            "has-evidence": [ 0 ],
            "evidence-data": [ "[]" ]
          },
          "completed_stages": [],
          "form": {
            "id": 1,
            "url": "http://b2d/api/v3/forms/1",
            "parent_id": null,
            "name": "Basic Post",
            "description": "Post with a location",
            "type": "report",
            "disabled": false,
            "updated": null,
            "allowed_privileges": [
              "read",
              "create",
              "update",
              "delete",
              "search"
            ]
          },
          "allowed_privileges": [
            "read",
            "create",
            "update",
            "delete",
            "search",
            "change_status"
          ]
        }""")
        post_json["title"] = first_tweet.textual_content
        post_json["content"] = first_tweet.textual_content
        post_json["created"] = int(time.mktime(thread.created_at.utctimetuple()))
        if thread.evidence is not None and len(thread.evidence) > 0:
            post_json["values"]["has-evidence"] = [ 1 ]
            post_json["values"]["evidence-data"] = [ dumps(thread.evidence) ] 
        
        logging.info(dumps(post_json))
        
        r = HTTPRequest(
          USH_BASEURL + "/api/v3/posts?order=desc&orderby=created",
          method='POST',
          headers={
            'Accept': 'application/json',
            'Authorization': 'Bearer %s' % self.access_token,
            'Content-Type': 'application/json'
            },
          body=dumps(post_json),
        )
        http_client = AsyncHTTPClient()
        response = yield http_client.fetch(r)
        if response.error:
            raise Exception("Bad create post response " + str(response))
        else:
            response = loads(response.body)
            raise gen.Return()

