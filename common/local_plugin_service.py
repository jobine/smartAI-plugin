import json
from time import gmtime, strftime, time
import os
import shutil
from flask import jsonify
from collections import namedtuple

from .util.timeutil import get_time_offset, str_to_dt, dt_to_str, get_time_list
from .util.csv import save_csv
from .util.data import get_metric_meta, do_verify, get_timeseries, upload_data
from .util.meta import insert_meta, get_entity, update_model
from .util.result import get_inference_result_id_list, save_inference_result_id
from .util.constant import STATUS_SUCCESS, STATUS_FAIL

from .tsanaclient import TSANAClient

from telemetry import log

from os import environ

import yaml


def try_except(fn):
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            log.error("-----Exception-----")
            return jsonify(dict(status=STATUS_FAIL, message='Unknown error, please check your request. ' + str(e))), 502

    return wrapped


def load_config(path):
    try:
        with open(path, 'r') as config_file:
            config_yaml = yaml.safe_load(config_file)
            Config = namedtuple('Config', sorted(config_yaml))
            config = Config(**config_yaml)
        return config
    except Exception:
        return None


class PluginService(object):
    @try_except
    def train(self, request, model_key):
        # check if the post request has the file part
        request_body = json.loads(request.data)
        subscription = request.headers.get('apim-subscription-id', 'Official')

        # Check if the model is existed and status is Active or retraining
        entity = get_entity(self.config, subscription, model_key)
        if entity == None:
            return jsonify(dict(status=STATUS_FAIL, message='Model is not found! ')), 400

        current_set = entity['series_set']
        current_para = entity['para']

        new_set = str(request_body['seriesSets'])
        new_para = str(request_body['instance']['params'])

        if current_set != new_set or current_para != new_para:
            return self.update(request, model_key, True)

        # result, message = do_verify(self.config, request_body, subscription)
        result, message = self.tsanaclient.do_verify(request_body, subscription)
        if result == STATUS_SUCCESS:
            result, message = self.do_train(request, model_key)
            return jsonify(dict(status=result, message=message)), 200
        else:
            return jsonify(dict(status=STATUS_FAIL, message='Verify failed! ' + message)), 400
    
    def do_train(self, request, model_key):
        return None, ''

    def prepare_data(self, parameters, model_key, time_key):
        inference_window = parameters['instance']['params']['windowSize']
        # meta = get_metric_meta(self.config, parameters['instance']['params']['target']['metricId'])
        meta = self.tsanaclient.get_metric_meta(parameters['instance']['params']['target']['metricId'])
        if meta is None:
            raise Exception('Metric is not found.')

        end_time = str_to_dt(parameters['endTime'])
        if 'startTime' in parameters:
            start_time = str_to_dt(parameters['startTime'])
        else:
            start_time = end_time

        data_end_time = get_time_offset(end_time, (meta['granularityName'], meta['granularityAmount']),
                                        + 1)
        data_start_time = get_time_offset(start_time, (meta['granularityName'], meta['granularityAmount']),
                                          - inference_window * 2)

        factor_def = parameters['seriesSets']
        # factors_data = get_timeseries(self.config, factor_def, data_start_time, data_end_time)
        factors_data = self.tsanaclient.get_timeseries(factor_def, data_start_time, data_end_time)

        target_def = [parameters['instance']['params']['target']]
        # target_data = get_timeseries(self.config, target_def, data_start_time, data_end_time)
        target_data = self.tsanaclient.get_timeseries(target_def, data_start_time, data_end_time)

        data_dir = os.path.join(self.config.model_data_dir, model_key, time_key)
        shutil.rmtree(data_dir, ignore_errors=True)
        os.makedirs(data_dir, exist_ok=True)

        for target in target_data:
            csv_file = os.path.join(data_dir, target.series_id + '.csv')
            save_csv([(tuple['timestamp'], tuple['value']) for tuple in target.value], csv_file)

        for factor in factors_data:
            csv_file = os.path.join(data_dir, factor.series_id + '.csv')
            save_csv([(tuple['timestamp'], tuple['value']) for tuple in factor.value], csv_file)

        return data_dir

    @try_except
    def inference(self, request, model_key):
        request_body = json.loads(request.data)
        subscription = request.headers.get('apim-subscription-id', 'Official')
        # result, message = do_verify(self.config, request_body, subscription)
        result, message = self.tsanaclient.do_verify(request_body, subscription)
        if result == STATUS_SUCCESS:
            time_key = strftime("%Y-%m-%d_%H%M%S", gmtime())
            data_dir = self.prepare_data(request_body, model_key, time_key)
            data_blob_info = upload_data(self.config, data_dir, model_key, time_key)
            request_body['data_blob_info'] = json.dumps(data_blob_info)
            request.data = json.dumps(request_body)
            timestamp, result_id, message = self.algoclient.inference(request, model_key)
            save_inference_result_id(self.config, model_key, timestamp, result_id)
            return jsonify(dict(status=STATUS_SUCCESS, message=message)), 200
        else:
            return jsonify(dict(status=STATUS_FAIL, message='Verify failed! ' + message)), 400

    def do_inference(self, request, model_key):
        pass

    @try_except
    def state(self, request, model_key):
        pass

    @try_except
    def list_models(self, request):
        return self.algoclient.list_models(request)

    @try_except
    def delete(self, request, model_key):
        result, message = self.algoclient.delete(request, model_key)
        if result == STATUS_SUCCESS:
            return jsonify(dict(instanceId=model_key, status=result)), 200
        else:
            return jsonify(dict(message=message, status=result)), 400