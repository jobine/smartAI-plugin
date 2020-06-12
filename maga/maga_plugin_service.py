import os
import json
from flask import jsonify
import uuid
import time
import shutil
from requests import Request

from common.plugin_service import PluginService
from common.util.constant import STATUS_SUCCESS, STATUS_FAIL
from common.util.constant import ModelState
from common.util.constant import InferenceState
from common.util.timeutil import dt_to_str, dt_to_str_file_name, str_to_dt
from common.util.csv import save_to_csv
from common.util.azureblob import AzureBlob

from .magaclient import MAGAClient

from concurrent.futures import ThreadPoolExecutor
import asyncio

from telemetry import log

# async infras
executor = ThreadPoolExecutor()
loop = asyncio.new_event_loop()

class MagaPluginService(PluginService):

    def __init__(self):
        super().__init__()
        self.magaclient = MAGAClient(self.config.maga_service_endpoint)

    def do_verify(self, parameters):
        return STATUS_SUCCESS, ''

    def do_train(self, subscritpion, model_key, model_dir, parameters):
        return STATUS_SUCCESS, ''

    def do_state(self, subscritpion, model_key):
        return STATUS_SUCCESS, ''

    def do_inference(self, subscritpion, model_key, model_dir, parameters):
        return STATUS_SUCCESS, ''

    def do_delete(self, subscritpion, model_key):
        return STATUS_SUCCESS, ''

    def prepare_training_data(self, parameters):
        end_time = str_to_dt(parameters['endTime'])
        if 'startTime' in parameters:
            start_time = str_to_dt(parameters['startTime'])
        else:
            start_time = end_time

        factor_def = parameters['seriesSets']
        factors_data = self.tsanaclient.get_timeseries(factor_def, start_time, end_time)

        time_key = dt_to_str_file_name(end_time)
        data_dir = os.path.join(self.config.model_data_dir, time_key, str(uuid.uuid1()))
        shutil.rmtree(data_dir, ignore_errors=True)
        os.makedirs(data_dir, exist_ok=True)

        variable = {}
        for factor in factors_data:
            csv_file = factor.series_id + '.csv'
            csv_data = []
            csv_data.append(('timestamp', 'value'))
            csv_data.extend([(tuple['timestamp'], tuple['value']) for tuple in factor.value])
            save_to_csv(csv_data, os.path.join(data_dir, csv_file))
            variable[factor.series_id] = csv_file
        
        zip_dir = os.path.abspath(os.path.join(data_dir, os.pardir))
        zip_file_base = os.path.join(zip_dir, 'training_data')
        zip_file = zip_file_base + '.zip'
        if os.path.exists(zip_file):
            os.remove(zip_file)
        shutil.make_archive(zip_file_base, 'zip', data_dir)

        azure_blob = AzureBlob(self.config.az_tsana_model_blob_connection)
        container_name = self.config.tsana_app_name
        azure_blob.create_container(container_name)

        blob_name = 'training_data_' + time_key
        with open(zip_file, "rb") as data:
            azure_blob.upload_blob(container_name, blob_name, data)

        os.remove(zip_file)
        blob_url = AzureBlob.generate_blob_sas(self.config.az_storage_account, self.config.az_storage_account_key, container_name, blob_name)

        result = {}
        result['variable'] = variable
        result['fillUpMode'] = parameters['instance']['params']['fillUpMode']
        result['tracebackWindow'] = parameters['instance']['params']['tracebackWindow']
        #result['source'] = blob_url
        result['source'] = '/data/training_data.zip'
        result['startTime'] = dt_to_str(start_time)
        result['endTime'] = dt_to_str(end_time)

        return result

    def prepare_inference_data(self, parameters):
        end_time = str_to_dt(parameters['endTime'])
        if 'startTime' in parameters:
            start_time = str_to_dt(parameters['startTime'])
        else:
            start_time = end_time

        factor_def = parameters['seriesSets']
        factors_data = self.tsanaclient.get_timeseries(factor_def, start_time, end_time)

        variable = {}
        for factor in factors_data:
            variable[factor.series_id] = factor.value

        result = {}
        result['data'] = variable
        result['startTime'] = dt_to_str(start_time)
        result['endTime'] = dt_to_str(end_time)
        return result

    def inference_wrapper(self, request, model_key, parameters, callback): 
        try:
            result = {}
            subrequest = Request()
            subrequest.headers = request.headers
            subrequest.data = parameters
            result = self.magaclient.inference(subrequest, model_key)
            if not result['resultId']:
                raise Exception(result['errorMessage'])
            
            resultId = result['resultId']
            while True:
                result = self.magaclient.get_result(subrequest, resultId)
                if result['status'] == 'READY':
                    break
        except Exception as e:
            result['errorMessage'] = str(e)

        if callback is not None:
            callback(request, model_key, result)

        return STATUS_SUCCESS, ''

    def inference_callback(self, request, model_key, result):
        log.info ("inference callback %s by %s, result = %s", model_key, request.headers.get('apim-subscription-id', 'Official'), result)
        return self.tsanaclient.save_inference_result(json.loads(request.data), result['result'])

    def train(self, request):
        request_body = json.loads(request.data)
        result, message = self.do_verify(request_body)
        if result != STATUS_SUCCESS:
            return jsonify(dict(status=STATUS_FAIL, message='Verify failed! ' + message)), 400
     
        request.data = self.prepare_training_data(request_body)

        return jsonify(dict(self.magaclient.train(request))), 200

    def inference(self, request, model_key):
        request_body = json.loads(request.data)
        result, message = self.do_verify(request_body)
        if result != STATUS_SUCCESS:
            return jsonify(dict(status=STATUS_FAIL, message='Verify failed! ' + message)), 400

        asyncio.ensure_future(loop.run_in_executor(executor, self.inference_wrapper, request, model_key, self.prepare_inference_data(request_body), self.inference_callback))
        return jsonify(dict(status=STATUS_SUCCESS, message='Inference task created')), 200

    def state(self, request, model_key):
        return jsonify(dict(self.magaclient.state(request, model_key))), 200

    def delete(self, request, model_key):
        return jsonify(dict(self.magaclient.delete_model(request, model_key))), 200
    
    def list_models(self, request):
        return jsonify(dict(self.magaclient.list_models(request))), 200