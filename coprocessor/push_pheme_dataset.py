#!/usr/bin/python
# -*- coding: utf-8 -*-
""" push_pheme_dataset.py: convert Pheme tagged rumour dataset into reports, 
reporters and messages, and push each of these to an ushahidi platform

Sara--Jayne Terp
David Losada Carballo
2015
"""

import os
import sys
import json
from glob import glob
from datetime import datetime

import ushapy.ushahidiv2 as ushapy
import dateutil
import dateutil.parser

mapurl = "http://pheme.ushahidi.com/"
secrets_file = "secrets.txt"

with open(secrets_file) as f:
    (ushuser, ushpass) = f.readline().strip().split(",")

'''Open files in directory. Expected structure is: 
Top-level directory:
    annotations/
        <lang>-scheme-annotations.json
    threads/
        <lang>/
            <event name>/
                <numbered directory>/ (one for each rumour thread)
                    who-follows-whom.dat
                    structure.json      <- retweet structure?
                    retweets.json       <- need explanation of this
                    annotation.json:    <- manual annotations
                        is_rumour       <- is the thread a rumour conversation?, always true (we are only given rumours)
                        category        <- story headline
                        misinformation  <- into metadata
                        true            <- is the story true
                        links           <- into metadata
                            link        <-
                            mediatype   <- into custom formfield
                            position    <- into custom formfield
                        is_turnaround   <- custom formfield (not sure what this is)
                    source-tweets/
                        <numbered json file>    <- single tweet
                    reactions/
                        <numbered json file(s)> <- single tweet per file


Tweet files: 
    filename is tweet id

'''

base_dir = sys.argv[1]
print "Using base_dir: %s" % base_dir

def to_compact_json(v):
    return json.dumps(v, separators=(',',':'), sort_keys=True)

# Start at the top.  Assume any directory found is the data directory.
class CategoryIds:
    def __init__(self, mapurl):
        self.mapurl = mapurl
        self.title_by_id = {}
        self.id_by_title = {}
        self._preload_cats()
    def _preload_cats(self):
        cats = ushapy.get_all_categories(self.mapurl)
        # First process categories without parents
        for cat in filter(lambda cat: cat['category']['parent_id'] == '0', cats):
            title = (cat['category']['title'],)     # tuple with one element
            the_id = int(cat['category']['id'])
            self.id_by_title[title] = the_id
            self.title_by_id[the_id] = title
        # Then process categories with parents
        for cat in filter(lambda cat: cat['category']['parent_id'] != '0', cats):
            parent_cat_id = int(cat['category']['parent_id'])
            parent_cat_title = self.title_by_id[parent_cat_id][0]
            the_id = int(cat['category']['id'])
            title = ( parent_cat_title, cat['category']['title'] )  # tuple with parent category name, this name
            self.id_by_title[title] = the_id
            self.title_by_id[the_id] = title
    def add_id(self, title, id):
        self.id_by_title[title] = int(id)
        self.title_by_id[int(id)] = title
    def get_id(self, title):
        if not self.id_by_title.has_key(title):
            return None
        else:
            return self.id_by_title[title]

class Reporters:
    def __init__(self, mapurl):
        self.mapurl = mapurl
        self.reporters = {}
    def find_or_create(self, handle, user_obj):
        if self.reporters.has_key(handle):
            return self.reporters[handle]
        else:
            reporter_obj = ushapy.get_reporter_by_service_account(mapurl, ushuser, ushpass, 3, handle)
            if reporter_obj is not None:
                self.reporters[handle] = int(reporter_obj['reporter_id'])
                return self.reporters[handle]
            else:
                reporter_id = self._create(handle, user_obj)
                self.reporters[handle] = reporter_id
                return self.reporters[handle]

    def _create(self, handle, user_obj):
        reporter = dict(
            service_id = 3,     # twitter
            service_account = user_obj['screen_name'],
            reporter_first = user_obj['name'],
            pheme_metadata = to_compact_json(user_obj)
            )
        r = ushapy.add_reporter_to_platform(mapurl, ushuser, ushpass, **reporter)
        print r.content
        resp = json.loads(r.content)
        if resp['payload']['success'] == 'false':
            print r.content
            raise Exception("API fail")
        return int(resp['payload']['id'])

category_ids = CategoryIds(mapurl)
reporters = Reporters(mapurl)

# Load the annotations
annotations = {}        # annotations by lang and tweet id
def load_annotations(lang):
    annotations[lang] = {}
    annot_file = os.path.join(base_dir, "annotations", "%s-scheme-annotations.json" % lang)
    # Assume one line per annotation
    with open(annot_file) as annot:
        for l in annot:
            l = l.strip()
            # some lines are comments or blank
            if len(l) == 0 or l.startswith('#'):
                continue
            # read json and save
            annot_data = json.loads(l)
            tweetid = annot_data['tweetid']
            annotations[lang][tweetid] = annot_data

def process_thread(lang, event, threadid):
    thread_folder = os.path.join(base_dir, "threads", lang, event, str(threadid))

    tweet_message_ids = {}      # from tweet id (str) to message_id in the db (int)

    # load thread annotations
    annot_file = os.path.join(thread_folder, "annotation.json")
    with open(annot_file, "r") as f: annotation = json.load(f)

    cat_ids = [ category_ids.get_id( (event,) ), ]

    # if there's an annotation category, ensure it exists (as a child of event category)
    if annotation.has_key('category') and annotation['category'] is not None and annotation['category'] != "":
        story_cat_title = (event, annotation['category'])
        story_cat_id = category_ids.get_id(story_cat_title)
        if story_cat_id is None:
            parent_cat_id = cat_ids[0]
            r = ushapy.add_category_to_map(mapurl, ushuser, ushpass, parent_id=cat_ids[0], category_title=annotation['category'], category_description=annotation['category'])
            print r.content
            story_cat_id = int(json.loads(r.content)['payload']['id'])
            category_ids.add_id(story_cat_title, story_cat_id)
        assert(story_cat_id is not None and story_cat_id > 0)
        cat_ids.append(story_cat_id)

    # load source tweet
    source_tweet_file = os.path.join(thread_folder, "source-tweets", "%s.json" % threadid)
    with open(source_tweet_file, "r") as f: source_tweet = json.load(f)
    source_tweet_annot = annotations[lang][threadid]

    source_tweet_created_at = dateutil.parser.parse(source_tweet['created_at'])

    # pick source tweet annotations
    # . create report
    report = dict(
            incident_title= source_tweet['text'],
            incident_description= source_tweet['text'],
            incident_category= cat_ids,
            incident_date= source_tweet_created_at.strftime("%m/%d/%Y"),
            incident_hour= source_tweet_created_at.strftime("%I"),
            incident_minute= source_tweet_created_at.strftime("%M"),
            incident_ampm= source_tweet_created_at.strftime("%p").lower(),
            is_rumour= 1,  # true
            service_id= 3, # twitter
        )
    if annotation.has_key('true'):
        report['is_true'] = 0 if annotation['true'] == 0 else 1
    if annotation.has_key('is_turnaround'):
        report['is_turnaround'] = 0 if annotation['is_turnaround'] == 0 else 1
    if annotation.has_key('misinformation') and annotation['misinformation'] in (1 , '1'):
        report['rumour_type'] = 'misinformation' 
    # Add metadata
    metadata = dict(
        retweet_count= source_tweet['retweet_count'],
        favorite_count= source_tweet['favorite_count'],
    )
    if source_tweet.has_key('entities') and source_tweet['entities'].has_key('urls'):
        metadata['entities_urls'] = source_tweet['entities']['urls']
    if annotation.has_key('links'):
        metadata['annotated_links'] = annotation['links']
    #
    report['pheme_metadata'] = to_compact_json(metadata)    # as JSON string

    print report
    r = ushapy.add_report_to_platform(mapurl, **report)
    print r.content
    resp = json.loads(r.content)
    if resp['payload']['success'] == 'false':
        print r.content
        raise Exception("API fail")
    else:
        report_id = int(resp['payload']['id'])
        # approve the report
        r = ushapy.approve_report(mapurl, ushuser, ushpass, report_id)
        print r.content
        resp = json.loads(r.content)
        if resp['payload']['success'] == 'false':
            print r.content
            raise Exception("API fail")

    # . create source tweet reporter
    source_reporter_handle = source_tweet['user']['screen_name']
    source_reporter_id = reporters.find_or_create(source_reporter_handle, source_tweet['user'])

    # . create source message
    message = dict(
         incident_id= report_id,
         message_text= source_tweet['text'],
         certainty = source_tweet_annot['certainty'],
         evidentiality = source_tweet_annot['evidentiality'],
         support_by_source = source_tweet_annot['support'],
         reporter_id= source_reporter_id,
         parent_id= 0,
         message_from= "",
         message_to = "",
         service_messageid= source_tweet['id'],
         message_detail = "",
         message_type= 0,
         message_date= source_tweet_created_at.strftime("%Y-%m-%d %H:%M:%S"),
         message_level = 0,
         latitude= 0,
         longitude= 0,
         location_id = 0,
         )
    message['pheme_metadata'] = to_compact_json(source_tweet)
    print message
    r = ushapy.add_message_to_platform(mapurl, ushuser,ushpass, **message)
    print r.content
    resp = json.loads(r.content)
    if resp['payload']['success'] == 'false':
        print r.content
        raise Exception("API fail")
    else:
        source_message_id = int(resp['payload']['id'])
        tweet_message_ids[threadid] = source_message_id

    # Create reaction messages
    # . load each reaction
    reaction_files = glob(os.path.join(thread_folder, 'reactions', '*.json'))
    for reaction_file in sorted(reaction_files):
        print "reading reaction from %s ..." % reaction_file
        with open(reaction_file, "r") as f: reaction_tweet = json.load(f)
        reaction_tweet_id = str(reaction_tweet['id'])
        reaction_reporter_handle = reaction_tweet['user']['screen_name']
        reaction_reporter_id = reporters.find_or_create(reaction_reporter_handle, reaction_tweet['user'])
        reaction_tweet_created_at = dateutil.parser.parse(reaction_tweet['created_at'])
        #
        reaction_tweet_annot = {}
        if annotations[lang].has_key(reaction_tweet_id):
            reaction_tweet_annot = annotations[lang][reaction_tweet_id]
        # parent_id defaults to source tweet unless we've already added the in_reply tweet
        parent_id = source_message_id
        if tweet_message_ids.has_key(reaction_tweet["in_reply_to_status_id_str"]):
            parent_id = tweet_message_ids[reaction_tweet["in_reply_to_status_id_str"]]
        #
        message = dict(
            incident_id= report_id,
            message_text= reaction_tweet['text'],
            reporter_id= reaction_reporter_id,
            parent_id= parent_id,
            message_from= "",
            message_to = "",
            service_messageid= reaction_tweet['id'],
            message_detail = "",
            message_type= 0,
            message_date= reaction_tweet_created_at.strftime("%Y-%m-%d %H:%M:%S"),
            message_level = 0,
            latitude= 0,
            longitude= 0,
            location_id = 0,
            )
        if reaction_tweet_annot.has_key('certainty'):
            message['certainty'] = reaction_tweet_annot['certainty']
        if reaction_tweet_annot.has_key('evidentiality'):
            message['evidentiality'] = reaction_tweet_annot['evidentiality']
        if reaction_tweet_annot.has_key('responsetype-vs-previous'):
            message['responsetype_vs_previous'] = reaction_tweet_annot['responsetype-vs-previous']
        if reaction_tweet_annot.has_key('responsetype-vs-source'):
            message['responsetype_vs_source'] = reaction_tweet_annot['responsetype-vs-source']
        #
        message['pheme_metadata'] = to_compact_json(reaction_tweet)
        print message
        r = ushapy.add_message_to_platform(mapurl, ushuser, ushpass, **message)
        print r.content
        resp = json.loads(r.content)
        if resp['payload']['success'] == 'false':
            print r.content
            raise Exception("API fail")
        else:
            message_id = int(resp['payload']['id'])
            tweet_message_ids[reaction_tweet_id] = message_id

def process_event(lang, event):
    event_folder = os.path.join(base_dir, "threads", lang, event)

    # Ensure that a category exists for the event
    cat_id = category_ids.get_id( (event,) )
    if cat_id is None:
        r = ushapy.add_category_to_map(mapurl, ushuser, ushpass, category_title=event, category_description=event)
        cat_id = int(json.loads(r.content)['payload']['id'])
        category_ids.add_id((event,), cat_id)
    assert(cat_id is not None and cat_id > 0)

    # For each thread in the event
    for thread_dir in glob(os.path.join(event_folder, "*")):
        if not os.path.isdir(thread_dir):
            continue
        threadid = thread_dir.split("/")[-1]
        print "lang %s - event %s - thread %s" % (lang, event, threadid)
        process_thread(lang, event, threadid)

def process_language(lang):
    load_annotations(lang)
    lang_folder = os.path.join(base_dir, "threads", lang)
    for event_dir in glob(os.path.join(lang_folder, "*")):
        if not os.path.isdir(event_dir):
            continue
        event = event_dir.split("/")[-1]
        print "lang %s - event %s" % (lang, event)
        process_event(lang, event)

process_language("en")

sys.exit(0)

#

