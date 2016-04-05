
#!/usr/bin/env python
""" Read/write to Ushahidi v2.7 instances

Ushapy requirements:
* Get all Ushahidi reports (id, title, description text, categories, formfield values, date), filtered by start/end dates, categories, form, formfields
* Get all Ushahidi messages (id, message text, source, reporterid, reportid), filtered by start/end dates, reporters
* Change category values on an Ushahidi report
* Change formfield values on an Ushahidi report
* Add categories and subcategories (allow user to hide these categories) to Platform
* Add report to platform

Sara-Jayne Terp
2014
"""

import requests
from requests.auth import HTTPBasicAuth
import time
import json
import datetime
#import read_write_csv


def add_category_to_map(mapurl, username, password, **kwargs):
    """
    known properties: parent_id, category_title, category_description, category_color, category_image
    """
    # required defaults
    if not kwargs.has_key("category_color"): kwargs["category_color"] = "000000"
    if not kwargs.has_key("parent_id"): kwargs["parent_id"] = 0
    if not kwargs.has_key("category_description"): kwargs["category_description"] = "created with ushapy"
    payload = { 'task': 'category', 'action': 'add' }
    payload.update(kwargs)

    r = requests.post(mapurl+"api", data=payload, auth=(username, password));

    return(r)


""" Change the categories list on an Ushahidi report

Parameters:
* mapurl: String containing URL of Ushahidi Platform instance, e.g. http://www.mymap.com/
* reportid: Ushahidi id for one of the Ushahidi Platform reports.
* adds: list of categories to add to the report
* removes: list of categories to remove from the report
* removeall: flag set to remove all categories from report, before adding the "adds" categories

"""
def change_report_categories(mapurl, reportid, adds=[], removes=[], removeall=False):
    return()



""" Get one report from ushahidi website, given its id and map url

Parameters:
* mapurl: String containing URL of Ushahidi Platform instance, e.g. http://www.mymap.com/
* reportid: Ushahidi id for one of the Ushahidi Platform reports.
* payload

"""
def get_ush_report(mapurl, reportid):
    #Put list of sites into a dictionary
    response = requests.get(url=mapurl+"api?task=incidents&by=incidentid&id="+str(reportid))
    reportsjson = json.loads(response.text)
    payload = reportsjson['payload']['incidents'][0]
    return(payload)


def get_reporter_by_service_account(mapurl, username, password, service_id, service_account):
    response = requests.get(url=mapurl+"api?task=reporters&by=serviceaccount&serviceid=%s&serviceaccount=%s" % (str(service_id), service_account), auth=(username, password))
    reportersjson = json.loads(response.text)
    if reportersjson['payload'].has_key('reporters'):
        return reportersjson['payload']['reporters'][0]['reporter']
    else:
        return None

""" Get reports from ushahidi website

Parameters:
* mapurl: String containing URL of Ushahidi Platform instance, e.g. http://www.mymap.com/
* reports

"""
def get_all_reports(mapurl):
    #Put list of sites into a dictionary
    reports = []
    numreports = get_number_of_ush_reports(mapurl)
    numcalls = numreports/100
    if numcalls < 100 or numcalls%100 != 0:
        numcalls += 1
    for call in range(0, numcalls):
        startid = str(call*100)
        if call == 0:
            response = requests.get(url=mapurl+"api?task=incidents&limit=100")  #Ush api crashes if sinceid=0
        else:
            response = requests.get(url=mapurl+"api?task=incidents&by=sinceid&id="+startid+"&limit=100")
        reportsjson = json.loads(response.text)
        for sitedetails in reportsjson['payload']['incidents']:
            reports += [sitedetails];
    return reports



def get_all_categories(mapurl):
    response = requests.get(url=mapurl+"api?task=categories")
    return json.loads(response.text)['payload']['categories']


""" Get number of reports on an Ushahidi Platform site

Parameters:
* mapurl: String containing URL of Ushahidi Platform instance, e.g. http://www.mymap.com/
* numreports: number of reports on the Ushahidi platform, at the time the call was made

"""
def get_number_of_ush_reports(mapurl):
    response = requests.get(url=mapurl+"api?task=incidentcount")
    numjson = json.loads(response.text)
    numreports = int(numjson['payload']['count'][0]['count'])
    return numreports



""" Use ushahidi site's categories list to convert text categories list into ids

Parameters:
* mapurl: String containing URL of Ushahidi Platform instance, e.g. http://www.mymap.com/
* catslist
* mapcategories
* catidslist

"""
def cats_to_catids(mapurl, catslist, mapcategories={}):
    catids = [];
    if mapcategories == {}:
        response = requests.get(url=mapurl+"api?task=categories");
        catsjson = json.loads(response.text);
        for cat in catsjson['payload']['categories']:
            mapcategories.setdefault(cat['category']['title'], cat['category']['id']);
    catnames = catslist.split(",");
    for catname in catnames:
        if catname in mapcategories:
            catids += [mapcategories[catname]];
    catidslist = ",".join(catids);
    return(catidslist)



""" Convert report output by Ushahidi report view API into a structure that can be
input into the Ushahidi report edit api.  Yes, yes, I know they should be the
same format, but life.

Parameters:
* viewpayload
* editload

"""
def reformat_ush_api_report_view_to_edit(viewpayload):
    editload = {}
    viewdetails = viewpayload['incident']
    viewcategories = viewpayload['categories']
    editload['incident_id'] = viewdetails['incidentid']
    editload['incident_title'] = viewdetails['incidenttitle']
    editload['incident_description'] = viewdetails['incidentdescription']
    editload['latitude'] = viewdetails['locationlatitude']
    editload['longitude'] = viewdetails['locationlongitude']
    editload['location_name'] = viewdetails['locationname']
    editload['location_id'] = viewdetails['locationid']
    editload['incident_active'] = viewdetails['incidentactive']
    editload['incident_verified'] = viewdetails['incidentverified']
    t = datetime.datetime.strptime(viewdetails['incidentdate'],  "%Y-%m-%d %H:%M:%S")
    editload['incident_date'] = t.strftime("%m/%d/%Y")
    editload['incident_hour'] = t.strftime("%I")
    editload['incident_minute'] = t.strftime("%M")
    editload['incident_ampm'] = t.strftime("%p").lower()
    cats = []
    for cat in viewcategories:
        cats += [str(cat['category']['id'])]
    editload['incident_category'] = ','.join(cats)
    return(editload)



""" Edit ushahidi report

Parameters:
* mapurl: String containing URL of Ushahidi Platform instance, e.g. http://www.mymap.com/
* r

"""
def edit_report(mapurl, payload, username, password):
    editload = payload
    editload['task'] = 'reports'
    editload['action'] = 'edit'
    r = requests.post(mapurl+"api?task=reports", data=editload, auth=(username, password));
    return(r)

def approve_report(mapurl, username, password, report_id):
    payload = { 'task': 'reports', 'action': 'approve', 'incident_id': report_id }
    return requests.post(mapurl+"api", data= payload, auth=(username, password));

def add_report_to_platform(mapurl, **kwargs):
    """
    known property names:
      incident_title, incident_description, incident_category,
      latitude, longitude, incident_date, incident_hour, incident_minute, incident_ampm, location_name
    """
    if not kwargs.has_key('latitude'): kwargs['latitude'] = 0
    if not kwargs.has_key('longitude'): kwargs['longitude'] = 0
    if not kwargs.has_key('location_name'): kwargs['location_name'] = "unknown"

    payload = { 'task': 'report' }
    payload.update(kwargs)

    r = requests.post(mapurl+"api", data=payload);

    return(r)

def add_message_to_platform(mapurl, username, password, **kwargs):
    """
    known property names:
      message_text, message_date, message_type,
      service_messageid, message_level, reporter_id, message_from, message_to, message_detail,
      parent_id, incident_id, latitude, longitude
    """

    # Make sure there's a timestamp on this message
    if not kwargs.has_key('message_date') or kwargs['message_date'] in (None, ''):
        now = time.gmtime()
        kwargs['message_date'] = time.strftime("%Y-%m-%d %H:%M:%S", now)

    payload = { 'task': 'messages', 'action': 'add' }
    payload.update(kwargs)

    r = requests.post(mapurl+"api", data=payload, auth=(username, password));
    return(r)


def add_reporter_to_platform(mapurl, username, password, **kwargs):
    """
    known property names:
      service_id, service_account, level_id, reporter_first, reporter_last,
      reporter_email, reporter_phone, reporter_ip, reporter_date, location_name, latitude, longitude
    """
    if not kwargs.has_key('location_id'): kwargs['location_id'] = ''
    if not kwargs.has_key('location_name'): kwargs['location_name'] = 'unknown'
    if not kwargs.has_key('latitude'): kwargs['latitude'] = 0
    if not kwargs.has_key('longitude'): kwargs['longitude'] = 0
    if not kwargs.has_key('level_id'): kwargs['level_id'] = 0


    payload = { 'task': 'reporters', 'action': 'add' }
    payload.update(kwargs)

    r = requests.post(mapurl+"api", data=payload, auth=(username, password));

    return(r)


def add_service_to_platform(mapurl, username, password, **kwargs):
    """
    known property names:
      service_name, service_description, service_url, service_api
    """

    payload = { 'task': 'services', 'action': 'add' }
    payload.update(kwargs)

    r = requests.post(mapurl+"api", data=payload, auth=(username, password));

    return(r)


def edit_service(mapurl, service_id, service_name, service_description, service_url, service_api,
    username, password, newservice=True):

    payload = { \
    'task': 'services', \
    'action': 'edit', \
    'service_id': service_id, \
    'service_name': service_name, \
    'service_description': service_description, \
    'service_url': service_url, \
    'service_api': service_api \
    };

    r = requests.post(mapurl+"api", data=payload, auth=(username, password));

    return(r)
