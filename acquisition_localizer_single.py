#!/usr/bin/env python 
from builtins import str
import os, sys, time, json, requests, logging

from hysds_commons.job_utils import resolve_hysds_job
from hysds.celery import app
from hysds_commons.job_utils import submit_hysds_job
import osaka.main

# set logger
log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


BASE_PATH = os.path.dirname(__file__)


def dataset_exists(id, index_suffix):
    """Query for existence of dataset by ID."""

    # es_url and es_index
    es_url = app.conf.GRQ_ES_URL
    es_index = "grq_*_{}".format(index_suffix.lower())
    
    # query
    query = {
        "query":{
            "bool":{
                "must":[
                    { "term":{ "_id": id } },
                ]
            }
        },
        "fields": [],
    }

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))
    if r.status_code == 200:
        result = r.json()
        total = result['hits']['total']
    else:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        if r.status_code == 404: total = 0
        else: 
            err_str = "Failed to query %s:\n%s" % (es_url, r.text)
            err_str += "\nreturned: %s" % r.text
            print(err_str)
            print("query: %s" % json.dumps(query, indent=2))
            #r.raise_for_status()
            raise RuntimeError(err_str)
    return False if total == 0 else True


def query_es(query, es_index):
    """Query ES."""

    es_url = app.conf.GRQ_ES_URL
    rest_url = es_url[:-1] if es_url.endswith('/') else es_url
    url = "{}/{}/_search?search_type=scan&scroll=60&size=100".format(rest_url, es_index)
    #logger.info("url: {}".format(url))
    r = requests.post(url, data=json.dumps(query))
    #r.raise_for_status()
    scan_result = r.json()
    #logger.info("scan_result: {}".format(json.dumps(scan_result, indent=2)))
    count = scan_result['hits']['total']
    scroll_id = scan_result['_scroll_id']
    hits = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        hits.extend(res['hits']['hits'])
    return hits


def query_aois(starttime, endtime):
    """Query ES for active AOIs that intersect starttime and endtime."""

    es_index = "grq_*_area_of_interest"
    query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "bool": {
                            "must": [
                                {
                                    "range": {
                                        "starttime": {
                                            "lte": endtime
                                        }
                                    }
                                },
                                {
                                    "range": {
                                        "endtime": {
                                            "gte": starttime
                                        }
                                    }
                                }
                            ]
                        }
                    },
                    {
                        "filtered": {
                            "query": {
                                "range": {
                                    "starttime": {
                                        "lte": endtime
                                    }
                                }
                            },
                            "filter": {
                                "missing": {
                                    "field": "endtime"
                                }
                            }
                        }
                    },
                    {
                        "filtered": {
                            "query": {
                                "range": {
                                    "endtime": {
                                        "gte": starttime
                                    }
                                }
                            },
                            "filter": {
                                "missing": {
                                    "field": "starttime"
                                }
                            }
                        }
                    }
                ]
            }
        },
        "partial_fields" : {
            "partial" : {
                "include" : [ "id", "starttime", "endtime", "location", 
                              "metadata.user_tags", "metadata.priority" ]
            }
        }
    }

    # filter inactive
    hits = [i['fields']['partial'][0] for i in query_es(query, es_index) 
            if 'inactive' not in i['fields']['partial'][0].get('metadata', {}).get('user_tags', [])]
    #logger.info("hits: {}".format(json.dumps(hits, indent=2)))
    logger.info("aois: {}".format(json.dumps([i['id'] for i in hits])))
    return hits


def query_aoi_acquisitions(starttime, endtime, platform):
    """Query ES for active AOIs that intersect starttime and endtime and 
       find acquisitions that intersect the AOI polygon for the platform."""

    acq_info = {}
    es_index = "grq_*_*acquisition*"
    for aoi in query_aois(starttime, endtime):
        logger.info("aoi: {}".format(aoi['id']))
        query = {
            "query": {
                "filtered": {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        "dataset_type.raw": "acquisition"
                                    }
                                },
                                {
                                    "term": {
                                        "metadata.platform.raw": platform
                                    }
                                },
                                {
                                    "range": {
                                        "starttime": {
                                            "lte": endtime
                                        }
                                    }
                                },
                                {
                                    "range": {
                                        "endtime": {
                                            "gte": starttime
                                        }
                                    }
                                }
                            ]
                        }
                    },
                    "filter": {
                        "geo_shape": {  
                            "location": {
                                "shape": aoi['location']
                            }
                        }
                    }
                }
            },
            "partial_fields" : {
                "partial" : {
                    "include" : [ "id", "dataset_type", "dataset", "metadata" ]
                }
            }
        }
        acqs = [i['fields']['partial'][0] for i in query_es(query, es_index)]
        logger.info("Found {} acqs for {}: {}".format(len(acqs), aoi['id'],
                    json.dumps([i['id'] for i in acqs], indent=2)))
        for acq in acqs:
            aoi_priority = aoi.get('metadata', {}).get('priority', 0)
            # ensure highest priority is assigned if multiple AOIs resolve the acquisition
            if acq['id'] in acq_info and acq_info[acq['id']].get('priority', 0) > aoi_priority:
                continue
            acq['aoi'] = aoi['id']
            acq['priority'] = aoi_priority
            acq_info[acq['id']] = acq
    logger.info("Acquistions to localize: {}".format(json.dumps(acq_info, indent=2)))
    return acq_info
    

def resolve_s1_slc(identifier, download_url, asf_queue, esa_queue):
    """Resolve S1 SLC using ASF datapool (ASF or NGAP). Fallback to ESA."""
    url_type = "asf"

    #asf_queue = "spyddder-sling-extract-asf"
    #esa_queue = "spyddder-sling-extract-scihub"

    # determine best url and corresponding queue by getting first 100 bytes
    vertex_url = "https://datapool.asf.alaska.edu/SLC/SA/{}.zip".format(identifier)
    headers = {"Range": "bytes=0-100"}
    r = requests.get(vertex_url, allow_redirects=True, headers=headers)
    logger.info("Status Code from ASF : %s" %r.status_code)
    if asf_queue.upper() != "NA" and r.status_code in (200, 206):
        url = r.url
        queue = asf_queue
        url_type = "asf"
    elif r.status_code == 404:
        url = download_url
        queue = esa_queue
        url_type = "scihub"
    else:
        url = download_url
        queue = esa_queue
        url_type = "scihub"
    if 'sonas.asf.alaska.edu' in url:
        url = download_url
        queue = esa_queue
        url_type = "scihub"
    #url = r.url
    #queue = asf_queue
        #raise RuntimeError("Got status code {} from {}: {}".format(r.status_code, vertex_url, r.url))
    return url, queue, url_type


class DatasetExists(Exception):
    """Exception class for existing dataset."""
    pass


def resolve_source_from_ctx(ctx):
    """Resolve best URL from acquisition."""

    dataset_type = ctx['dataset_type']
    identifier = ctx['identifier']
    dataset = ctx['dataset']
    download_url = ctx['download_url']
    asf_ngap_download_queue = ctx['asf_ngap_download_queue']
    esa_download_queue = ctx['esa_download_queue']
    job_priority = ctx.get('job_priority', 0)
    aoi = ctx.get('aoi', 'no_aoi')
    spyddder_extract_version = ctx['spyddder_extract_version']
    archive_filename = ctx['archive_filename']

    return resolve_source(dataset_type, identifier, dataset, download_url, asf_ngap_download_queue, esa_download_queue, spyddder_extract_version,archive_filename, job_priority, aoi)


def resolve_source(dataset_type, identifier, dataset, download_url, asf_ngap_download_queue, esa_download_queue, spyddder_extract_version, archive_filename, job_priority, aoi):
   
    # get settings
    '''
    settings_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'settings.json')
    with open(settings_file) as f:
        settings = json.load(f)
    '''
    settings = {}
    settings['ACQ_TO_DSET_MAP'] = {"acquisition-S1-IW_SLC": "S1-IW_SLC"}
   

    # ensure acquisition
    if dataset_type != "acquisition":
        raise RuntimeError("Invalid dataset type: {}".format(dataset_type))

    # route resolver and return url and queue
    if dataset == "acquisition-S1-IW_SLC":
        '''
        if dataset_exists(identifier, settings['ACQ_TO_DSET_MAP'][dataset]):
            raise DatasetExists("Dataset {} already exists.".format(identifier))
        '''
        url, queue, url_type = resolve_s1_slc(identifier, download_url, asf_ngap_download_queue, esa_download_queue)
    else:
        raise RuntimeError("Unknown acquisition dataset: {}".format(dataset))

    try:
        #return extract_job(spyddder_extract_version, queue, url, archive_filename, identifier, time.strftime('%Y-%m-%d' ), job_priority, aoi)
        return sling_extract_job(spyddder_extract_version, identifier, url_type, download_url, queue, archive_filename,  
                time.strftime('%Y-%m-%d' ), job_priority, aoi)
    except Exception as err:
        err_msg = "ERROR running sling_extract_job : %s" %str(err)
        logger.info(err_msg)
        traceback.print_exc()
        raise RuntimeError(err_msg)

        #return extract_job(spyddder_extract_version, queue, url, archive_filename, identifier, time.strftime('%Y-%m-%d' ), job_priority, aoi)

def resolve_source_from_ctx_file(ctx_file):
    """Resolve best URL from acquisition."""

    with open(ctx_file) as f:
        return resolve_source_from_ctx(json.load(f))

def sling_extract_job(sling_extract_version, slc_id, url_type, download_url, queue, archive_file,
                prod_date, priority, aoi, wuid=None, job_num=None):
    """Map function for spyddder-man extract job."""

    # set job type and disk space reqs
    #job_type = "job-spyddder-extract:{}".format(spyddder_extract_version)
    logger.info("\nsling_extract_job for :%s" %slc_id)
    job_type = "job-spyddder-sling-extract-{}:{}".format(url_type, sling_extract_version)

    # resolve hysds job
    params = {
        "slc_id": slc_id
    }


    job = resolve_hysds_job(job_type, queue, priority=priority, params=params,
                            job_name="%s-%s" % (job_type, slc_id))


    # add workflow info
    #if wuid is not None and job_num is not None:
    job['payload']['_sciflo_wuid'] = wuid
    job['payload']['_sciflo_job_num'] = job_num
    #print("job: {}".format(json.dumps(job, indent=2)))

    return submit_hysds_job(job)


def extract_job(spyddder_extract_version, queue, localize_url, file, prod_name,
                prod_date, priority, aoi, wuid=None, job_num=None):
    """Map function for spyddder-man extract job."""

    '''
    if wuid is None or job_num is None:
        raise RuntimeError("Need to specify workunit id and job num.")
    '''

    # set job type and disk space reqs
    job_type = "job-spyddder-extract:{}".format(spyddder_extract_version)
    #job_type = "job-spyddder-sling-extract:{}".format(spyddder_extract_version)

    # resolve hysds job
    params = {
        "localize_url": localize_url,
        "file": file,
        "prod_name": prod_name,
        "prod_date": prod_date,
        "aoi": aoi,
    }

     
    job = resolve_hysds_job(job_type, queue, priority=priority, params=params, 
                            job_name="%s-%s" % (job_type, prod_name))

    # save to archive_filename if it doesn't match url basenamea
    '''
    localize_urls =  [
      {
        "local_path": file, 
        "url": localize_url
      }
    ]
    job['payload']['localize_urls'] = localize_urls
    '''

    if os.path.basename(localize_url) != file:
        job['payload']['localize_urls'][0]['local_path'] = file
    

    # add workflow info
    #if wuid is not None and job_num is not None:
    job['payload']['_sciflo_wuid'] = wuid
    job['payload']['_sciflo_job_num'] = job_num
    #print("job: {}".format(json.dumps(job, indent=2)))

    return submit_hysds_job(job)

