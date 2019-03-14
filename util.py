#!/usr/bin/env python 
import os, sys, time, json, requests, logging
from hysds_commons.job_utils import resolve_hysds_job
from hysds.celery import app
import datetime
import dateutil.parser
from datetime import datetime, timedelta

GRQ_URL = app.conf.GRQ_ES_URL



def sling_extract_job(sling_extract_version, slc_id, url_type, download_url, queue, file, 
                prod_date, priority, aoi, wuid=None, job_num=None):
    """Map function for spyddder-man extract job."""

    '''
    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")
    '''

    # set job type and disk space reqs
    #job_type = "job-spyddder-extract:{}".format(spyddder_extract_version)
    job_type = "job-spyddder-sling-extract:{}".format(sling_extract_version)

    # resolve hysds job
    params = {
        "slc_id": slc_id,
        "source" : url_type,
        "download_url" : download_url,
        "file": file,
        "prod_name": slc_id,
        "prod_date": prod_date,
        "aoi": aoi,
    }


    job = resolve_hysds_job(job_type, queue, priority=priority, params=params,
                            job_name="%s-%s" % (job_type, slc_id))

    # save to archive_filename if it doesn't match url basenamea
    '''
    localize_urls =  [
      {
        "local_path": file, 
        "url": localize_url
      }
    ]
    job['payload']['localize_urls'] = localize_urls
    
   
    if os.path.basename(localize_url) != file:
        job['payload']['localize_urls'][0]['local_path'] = file
    '''

    # add workflow info
    #if wuid is not None and job_num is not None:
    job['payload']['_sciflo_wuid'] = wuid
    job['payload']['_sciflo_job_num'] = job_num
    #print("job: {}".format(json.dumps(job, indent=2)))

    return submit_hysds_job(job)


def get_dataset(id, index_suffix):
    """Query for existence of dataset by ID."""

    # es_url and es_index
    es_url = GRQ_URL
    es_index = "grq_*_{}".format(index_suffix.lower())
    #es_index = "grq"

    # query
    query = {
        "query":{
            "bool":{
                "must":[
                    { "term":{ "_id": id } }
                ]
            }
        },
        "fields": []
    }

    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    print(result['hits']['total'])
    return result

def get_dataset(id):
    """Query for existence of dataset by ID."""

    # es_url and es_index
    es_url = GRQ_URL
    #es_index = "grq_*_{}".format(index_suffix.lower())
    es_index = "grq"

    # query
    query = {
        "query":{
            "bool":{
                "must":[
                    { "term":{ "_id": id } }
                ]
            }
        },
        "fields": []
    }

    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    print(result['hits']['total'])
    return result


def get_partial_grq_data(id):
    es_url = GRQ_URL
    es_index = "grq"

    query = {
        "query": {
            "term": {
                "_id": id,
            },
        },
        "partial_fields" : {
            "partial" : {
                "exclude" : "city",
            }
        }
    }

    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    print(result['hits']['total'])
    return result['hits']['hits'][0]


def get_query_data(query):
    es_url = GRQ_URL
    es_index = "grq"

    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    print(result['hits']['total'])
    return result['hits']


def get_acquisition_data(id):
    es_url = GRQ_URL
    es_index = "grq_*_*acquisition*"
    query = {
      "query": {
        "bool": {
          "must": [
            {
              "term": {
                "_id": id
              }
            }
          ]
        }
      },
      "partial_fields": {
        "partial": {
          "include": [
            "id",
            "dataset_type",
            "dataset",
            "metadata",
            "city",
            "continent"
          ]
        }
      }
    }


    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()
    print(result['hits']['total'])
    return result['hits']['hits']


