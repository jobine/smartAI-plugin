import json
import datetime
import traceback
import sys

from common.util.timeutil import get_time_offset, str_to_dt, dt_to_str
from common.util.series import Series
from common.util.retryrequests import RetryRequests
from common.util.constant import STATUS_SUCCESS, STATUS_FAIL

from telemetry import log

REQUEST_TIMEOUT_SECONDS = 30


class TSANAClient(object):
    def __init__(self, endpoint, api_key, series_limit, username=None, password=None, retrycount=3, retryinterval=1000):
        self.endpoint = endpoint
        self.api_key = api_key
        self.series_limit = series_limit
        self.username = username
        self.password = password
        self.retryrequests = RetryRequests(retrycount, retryinterval)

    def post(self, path, data):
        url = self.endpoint + path
        headers = {
            "x-api-key": self.api_key,
            "Content-Type": "application/json"
        }

        if self.username and self.password:
            auth = (self.username, self.password)
        else:
            auth = None

        try:
            r = self.retryrequests.post(url=url, headers=headers, auth=auth, data=json.dumps(data),
                                        timeout=REQUEST_TIMEOUT_SECONDS, verify=False)
            return r.json()
        except Exception as e:
            raise Exception('TSANA service api "{}" failed, request:{}, {}'.format(path, data, str(e)))

    def get(self, path):
        url = self.endpoint + path
        headers = {
            "x-api-key": self.api_key,
            "Content-Type": "application/json"
        }

        if self.username and self.password:
            auth = (self.username, self.password)
        else:
            auth = None

        try:
            r = self.retryrequests.get(url=url, headers=headers, auth=auth, timeout=REQUEST_TIMEOUT_SECONDS,
                                       verify=False)
            return r.json()
        except Exception as e:
            raise Exception('TSANA service api "{}" failed, {}'.format(path, str(e)))

    # To get the meta of a specific metric from TSANA
    # Parameters:
    #   config: a dict object which should include TSANA_API_KEY, TSANA_API_ENDPOINT, SERIES_LIMIT
    #   metric_id: a UUID string
    # Return:
    #   meta: the meta of the specified metric, or None if there is something wrong. 
    def get_metric_meta(self, metric_id):
        return self.get('/metrics/' + metric_id + '/meta')

    def get_dimesion_values(self, metric_id, dimension_name):
        dims = self.get('/metrics/' + metric_id + '/dimensions')
        if 'dimensions' in dims and dimension_name in dims['dimensions']:
            return dims['dimensions'][dimension_name]
        else:
            return None

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
    def get_timeseries(self, series_sets, start_time, end_time, offset=0, granularityName=None, granularityAmount=0,
                       top=1):
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

            para = dict(metricId=data['metricId'], dimensions=dim, count=top, startTime=start_str)
            ret = self.post('/metrics/' + data['metricId'] + '/rank-series', data=para)
            for s in ret['value']:
                if s['seriesId'] not in dedup:
                    s['startTime'] = start_str
                    s['endTime'] = end_str
                    s['dimension'] = s['dimensions']
                    del s['dimensions']
                    series.append(s)
                    dedup[s['seriesId']] = True

        # Query the data
        multi_series_data = None
        if len(series) > 0:
            ret = self.post('/metrics/series/data', data=dict(value=series))
            if granularityName is not None:
                multi_series_data = [
                    Series(factor['id']['metricId'], factor['id']['seriesId'], factor['id']['dimension'],
                           [dict(timestamp=get_time_offset(str_to_dt(y[0]), (granularityName, granularityAmount),
                                                           offset)
                                 , value=y[1])
                            for y in factor['values']])
                    for factor in ret['value']
                ]
            else:
                multi_series_data = [
                    Series(factor['id']['metricId'], factor['id']['seriesId'], factor['id']['dimension'],
                           value=[dict(timestamp=y[0]
                                       , value=y[1])
                                  for y in factor['values']])
                    for factor in ret['value']]
        else:
            log.info("Series is empty")

        return multi_series_data

    # Save a result back to TSANA
    # Parameters: 
    #   config: a dict object which should include AZ_STORAGE_ACCOUNT, AZ_STORAGE_ACCOUNT_KEY, AZ_META_TABLE
    #   parameters: 
    #        groupId: groupId in TSANA, which is copied from inference request, or from the entity
    #        instance: instance object, which is copied from the inference request, or from the entity
    #   result: an array of inference result. 
    # Return:
    #   result: STATE_SUCCESS / STATE_FAIL
    #   messagee: description for the result 
    def save_inference_result(self, parameters, result):
        try: 

            if len(result) <= 0: 
                return STATUS_SUCCESS, ''

            body = {
                'groupId': parameters['groupId'], 
                'instanceId': parameters['instance']['instanceId'], 
                'results': []
            }

            for item in result:
                item['timestamp'] = dt_to_str(str_to_dt(item['timestamp']))
                body['results'].append({
                    'params': parameters['instance']['params'],
                    'timestamp': item['timestamp'], 
                    'result': item, 
                    'createdTime': dt_to_str(datetime.datetime.now())
                })

            self.post('/timeSeriesGroups/' + parameters['groupId'] + '/appInstances/' + parameters['instance']['instanceId'] + '/saveResult', body)
            return STATUS_SUCCESS, ''
        except Exception as e: 
            traceback.print_exc(file=sys.stdout)
            return STATUS_FAIL, str(e)

    # Save a result back to TSANA
    # Parameters: 
    #   config: a dict object which should include AZ_STORAGE_ACCOUNT, AZ_STORAGE_ACCOUNT_KEY, AZ_META_TABLE
    #   parameters: 
    #        groupId: groupId in TSANA, which is copied from inference request, or from the entity
    #        instance: instance object, which is copied from the inference request, or from the entity
    #   result: an array of inference result. 
    # Return:
    #   result: STATE_SUCCESS / STATE_FAIL
    #   messagee: description for the result 
    def save_data_points(self, parameters, metricId, dimensions, timestamps, values):
        try: 
            if len(values) <= 0: 
                return STATUS_SUCCESS, ''

            body = {
                "metricId": metricId, 
                "dimensions": dimensions,
                "timestamps": timestamps, 
                "values": values
            }
            print(json.dumps(body))

            self.post('/pushData', body)
            return STATUS_SUCCESS, ''
        except Exception as e: 
            traceback.print_exc(file=sys.stdout)
            return STATUS_FAIL, str(e)


    def get_inference_result(self, parameters, start_time, end_time):
        try: 
            ret = self.get('/timeSeriesGroups/' 
                                + parameters['groupId'] 
                                + '/appInstances/' 
                                + parameters['instance']['instanceId'] 
                                + '/history?startTime=' 
                                + dt_to_str(start_time)
                                + '&endTime=' 
                                + dt_to_str(end_time))
            
            return STATUS_SUCCESS, '', ret
        except Exception as e: 
            traceback.print_exc(file=sys.stdout)
            return STATUS_FAIL, str(e), None