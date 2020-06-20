import os
import json
from flask import jsonify, make_response
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
from common.util.meta import get_meta, update_state, get_model_list, clear_state_when_necessary

from maga.magaclient import MAGAClient

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

    def do_verify(self, subscription, parameters):
        return STATUS_SUCCESS, ''

    def do_train(self, subscription, model_key, model_dir, parameters):
        request = Request()
        request.headers['apim-subscription-id'] = subscription
        request.data = self.prepare_training_data(parameters)
        result = self.magaclient.train(request)
        if 'modelId' in result:
            update_state(self.config, subscription, model_key, ModelState.TRAINING, json.dumps(result), None)

            while True:
                state = self.magaclient.state(request, result['modelId'])
                if state['summary']['status'] != 'CREATED' and state['summary']['status'] != 'RUNNING':
                    break
                else:
                    update_state(self.config, subscription, model_key, ModelState.TRAINING, json.dumps(state), None)
                    time.sleep(5)
            
            if state['summary']['status'] == 'READY':
                update_state(self.config, subscription, model_key, ModelState.READY, json.dumps(state), None)
                return STATUS_SUCCESS, json.dumps(state)
            else:
                update_state(self.config, subscription, model_key, ModelState.FAILED, json.dumps(state), None)
                return STATUS_FAIL, json.dumps(state)
        else:
            update_state(self.config, subscription, model_key, ModelState.FAILED, json.dumps(result), result['message'])
            return STATUS_FAIL, result['message']

    def do_state(self, subscription, model_key):
        return STATUS_SUCCESS, ''

    def do_inference(self, subscription, model_key, model_dir, parameters):
        request = Request()
        request.headers['apim-subscription-id'] = subscription
        request.data = self.prepare_inference_data(parameters)
        
        meta = get_meta(self.config, subscription, model_key)
        context = json.loads(meta['context'])
        actual_model_key = context['modelId']
        result = self.magaclient.inference(request, actual_model_key)
        if not result['resultId']:
            raise Exception(result['errorMessage'])
        
        resultId = result['resultId']
        while True:
            result = self.magaclient.get_result(request, resultId)
            if result['statusSummary']['status'] == 'READY':
                break
            else:
                time.sleep(5)

        return STATUS_SUCCESS, result

    def do_delete(self, subscription, model_key):
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

        try:
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
            result['mergeMode'] = parameters['instance']['params']['mergeMode']
            result['tracebackWindow'] = parameters['instance']['params']['tracebackWindow']
            result['source'] = blob_url
            result['startTime'] = dt_to_str(start_time)
            result['endTime'] = dt_to_str(end_time)

            return result
        finally:
            shutil.rmtree(data_dir, ignore_errors=True)        

    def prepare_inference_data(self, parameters):
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

        try:
            for factor in factors_data:
                csv_file = factor.series_id + '.csv'
                csv_data = []
                csv_data.append(('timestamp', 'value'))
                csv_data.extend([(tuple['timestamp'], tuple['value']) for tuple in factor.value])
                save_to_csv(csv_data, os.path.join(data_dir, csv_file))
            
            zip_dir = os.path.abspath(os.path.join(data_dir, os.pardir))
            zip_file_base = os.path.join(zip_dir, 'inference_data')
            zip_file = zip_file_base + '.zip'
            if os.path.exists(zip_file):
                os.remove(zip_file)
            shutil.make_archive(zip_file_base, 'zip', data_dir)

            azure_blob = AzureBlob(self.config.az_tsana_model_blob_connection)
            container_name = self.config.tsana_app_name
            azure_blob.create_container(container_name)

            blob_name = 'inference_data_' + time_key
            with open(zip_file, "rb") as data:
                azure_blob.upload_blob(container_name, blob_name, data)

            os.remove(zip_file)
            blob_url = AzureBlob.generate_blob_sas(self.config.az_storage_account, self.config.az_storage_account_key, container_name, blob_name)

            result = {}
            result['source'] = blob_url
            result['startTime'] = dt_to_str(start_time)
            result['endTime'] = dt_to_str(end_time)
            return result
        finally:
            shutil.rmtree(data_dir, ignore_errors=True)        

    def inference_wrapper(self, subscription, model_key, parameters, timekey, callback): 
        log.info("Start inference wrapper %s by %s ", model_key, subscription)
        try:
            result = {}
            prd_dir = os.path.join(self.config.model_temp_dir, subscription + '_' + model_key)
            status, result = self.do_inference(subscription, model_key, prd_dir, parameters)

            # TODO: Write the result back
            log.info("Inference result here: %s" % result)
            if callback is not None:
                callback(subscription, model_key, parameters, timekey, result)    
        except Exception as e:
            if callback is not None:
                callback(subscription, model_key, parameters, timekey, result, str(e))

        return STATUS_SUCCESS, ''

    def train_callback(self, subscription, model_key, model_state, timekey, last_error=None):
        log.info("Training callback %s by %s , state = %s", model_key, subscription, model_state)
        meta = get_meta(self.config, subscription, model_key)
        if meta is None or meta['state'] == ModelState.DELETED.name:
            return STATUS_FAIL, 'Model is not found! '

        return update_state(self.config, subscription, model_key, model_state, None, last_error)

    def inference_callback(self, subscription, model_key, parameters, timekey, result, last_error=None):
        log.info ("inference callback %s by %s, result = %s", model_key, subscription, result)
        return self.tsanaclient.save_inference_result(parameters, result['result'])
 
    def state(self, request, model_key):
        subscription = request.headers.get('apim-subscription-id', 'Official')
        meta = get_meta(self.config, subscription, model_key)
        context = json.loads(meta['context'])
        actual_model_key = context['modelId']
        state = self.magaclient.state(request, actual_model_key)
        
        if state['summary']['status'] == 'RUNNING':
            model_state = ModelState.TRAINING
        elif state['summary']['status'] == 'READY':
            model_state = ModelState.READY
        elif state['summary']['status'] == 'DELETED':
            model_state = ModelState.DELETED
        else:
            model_state = ModelState.FAILED
        
        update_state(self.config, subscription, model_key, model_state, json.dumps(state), None)

        return make_response(jsonify(dict(status=STATUS_SUCCESS, modelState=model_state.name, message=json.dumps(state))), 200)

    def delete(self, request, model_key):
        subscription = request.headers.get('apim-subscription-id', 'Official')
        meta = get_meta(self.config, subscription, model_key)
        actual_model_key = meta['context']['modelId']
        self.magaclient.delete_model(request, actual_model_key)
        super().delete(request, model_key)
        return make_response(jsonify(dict(status=STATUS_SUCCESS, message='Model {} has been deleted'.format(model_key))), 200)