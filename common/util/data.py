import requests
import json
import os
import sys
import shutil

from .azureblob import AzureBlob
from .azuretable import AzureTable
from .timeutil import get_time_offset, str_to_dt, dt_to_str
from .series import Series
from .constant import STATUS_SUCCESS, STATUS_FAIL

from telemetry import log

# To get the meta of a specific metric from TSANA
# Parameters:]
#   config: a dict object which should include TSANA_API_KEY, TSANA_API_ENDPOINT, SERIES_LIMIT
#   metric_id: a UUID string
# Return:
#   meta: the meta of the specified metric, or None if there is something wrong. 
def get_metric_meta(config, metric_id):
    headers = {
        "x-api-key": config.tsana_api_key, 
        "Content-Type": "application/json"
    }
    response = requests.get(config.tsana_api_endpoint + '/metrics/' + metric_id + '/meta', headers = headers)
    if response.status_code == 200:
        return response.json()
    else: 
        return None

# Verify if the data could be used for this application
# Parameters: 
#   series_sets: a array of series set
#   parameters: parameters of this application.
# Return:
#   result:  STATUS_FAIL / STATUS_SUCCESS
#   message: a description of the result
def do_verify(config, parameters, subscription):
    # common headers
    headers = {
        # The key to access TSANA
        "x-api-key": config.tsana_api_key, 
        "Content-Type": "application/json"
    }

    # ------TO BE REPLACED: Other application just replace below part-------
    # For forecast, check the factors and target has same granularity, and each factor could only contain one series
    meta = get_metric_meta(config, parameters['instance']['params']['target']['metricId'])
    if meta is None: 
        return STATUS_FAIL, 'Target is not found. '
    target_gran = meta['granularityName']
    # Only for custom, the granularity amount is meaningful which is the number of seconds
    target_gran_amount = meta['granularityAmount']

    for data in parameters['seriesSets']: 
        if target_gran != data['metricMeta']['granularityName'] or (target_gran == 'Custom' and target_gran_amount != data['metricMeta']['granularityAmount']):
            return STATUS_FAIL, 'Granularity must be identical between target and factors. '

    # Check series count, and each factor should only contain 1 series
    seriesCount = 0
    for data in parameters['seriesSets']: 
        dim = {}
        for dimkey in data['dimensionFilter']: 
            dim[dimkey] = [data['dimensionFilter'][dimkey]]
        
        dt = dt_to_str(str_to_dt(meta['dataStartFrom']))
        para = dict(metricId=data['metricId'], dimensions=dim, count=2, startTime=dt)     # Let's said 100 is your limitation
        response = requests.post(config.tsana_api_endpoint + '/metrics/' + data['metricId'] + '/rank-series', data = json.dumps(para), headers = headers)
        ret = response.json()
        if ret is None or response.status_code != 200 or 'value' not in ret:
            return STATUS_FAIL, 'Read series rank filed. '
        seriesCount += len(ret['value'])
        if seriesCount > config.series_limit:
            return STATUS_FAIL, 'Cannot accept ambiguous factors or too many series in the group, limit is ' + str(config.series_limit) + '.'

    return STATUS_SUCCESS, ''

# Query time series from TSANA
# Parameters: 
#   config: a dict object which should include TSANA_API_KEY, TSANA_API_ENDPOINT
#   series_sets: Array of series set
#   start_time: inclusive, the first timestamp to be query
#   end_time: exclusive
#   offset: a number will be added to each timestamp of each time-series. The unit is defined by granularity
#   granularityName: if Offset > 0, the granularityName is Monthly / Weekly / Daily / Hourly / Minutely / Secondly / Custom
#   granularityAmount: if granularityName is Custom, granularityAmount is the seconds of the exact granularity
# Return: 
#   A array of Series object
def get_timeseries(config, series_sets, start_time, end_time, offset = 0, granularityName = None, granularityAmount = 0): 
    # common headers
    headers = {
        "x-api-key": config.tsana_api_key, 
        "Content-Type": "application/json"
    }

    if offset != 0 and granularityName is None:
        offset = 0
        
    end_str = dt_to_str(end_time)
    start_str = dt_to_str(start_time)
    dedup = {}
    series = []

    # Query each series's tag
    for data in series_sets: 
        dim = {}
        if 'dimensionFilter' not in data:
            data['dimensionFilter'] = data['filters']
            
        for dimkey in data['dimensionFilter']: 
            dim[dimkey] = [data['dimensionFilter'][dimkey]]

        para = dict(metricId=data['metricId'], dimensions=dim, count=1, startTime=start_str, endTime=end_str)  
        response = requests.post(config.tsana_api_endpoint + '/metrics/' + data['metricId'] + '/rank-series', data = json.dumps(para), headers = headers)
        if response.status_code == 200: 
            ret = response.json()
            
            for s in ret['value']:
                if s['seriesId'] not in dedup:
                    s['startTime'] = start_str
                    s['endTime'] = end_str
                    s['dimension'] = s['dimensions']
                    del s['dimensions']
                    series.append(s)
                    dedup[s['seriesId']] = True
        else: 
            log.info("Fail to call rank %s", json.dumps(para))
            return None

    # Query the data
    multi_series_data = None
    if len(series) > 0: 
        response = requests.post(config.tsana_api_endpoint + '/metrics/series/data', data = json.dumps(dict(value=series)), headers = headers)
        if response.status_code == 200: 
            ret = response.json()
            if granularityName is not None:
                multi_series_data = [
                    Series(factor['id']['metricId'], factor['id']['seriesId'], factor['id']['dimension'], 
                                    [dict(timestamp = get_time_offset(str_to_dt(y[0]), (granularityName, granularityAmount),
                                                                    offset)
                                                , value = y[1]) 
                                                for y in factor['values']])
                    for factor in ret['value']
                ]
            else: 
                multi_series_data = [
                    Series(factor['id']['metricId'], factor['id']['seriesId'], factor['id']['dimension'], 
                                    value = [dict(timestamp = y[0]
                                                , value = y[1]) 
                                                for y in factor['values']])
                    for factor in ret['value']
                ]                    
        else: 
            log.info("Fail to call %s ", json.dumps(para))
    else: 
        log.info("Series is empty")
    
    return multi_series_data

def upload_data(config, data_dir, model_key, time_key):
    zip_file_base = os.path.join(config.model_temp_dir, 'data')
    zip_file = zip_file_base + '.zip'
    if os.path.exists(zip_file):
        os.remove(zip_file)
    shutil.make_archive(zip_file_base, 'zip', data_dir)

    azure_blob = AzureBlob(config.az_tsana_model_blob_connection)
    container_name = config.tsana_app_name
    blob_name = model_key + '_' + time_key
    
    try:
        azure_blob.create_container(container_name)
    except:
        print("Unexpected error:", sys.exc_info()[0])

    with open(zip_file, "rb") as data:
        azure_blob.upload_blob(container_name, blob_name, data)

    os.remove(zip_file)

    data_blob_info = {}
    data_blob_info['az_blob_connection'] = config.az_tsana_model_blob_connection
    data_blob_info['container_name'] = container_name
    data_blob_info['blob_name'] = blob_name
    return data_blob_info