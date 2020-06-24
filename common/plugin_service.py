import json
import time
from time import gmtime, strftime
import os
import shutil
from flask import jsonify, make_response
from collections import namedtuple
import uuid

from common.util.timeutil import get_time_offset, str_to_dt, dt_to_str, get_time_list
from common.util.meta import insert_meta, get_meta, update_state, get_model_list, clear_state_when_necessary
from common.util.model import copy_tree_and_zip_and_update_remote, prepare_model
from common.util.constant import STATUS_SUCCESS, STATUS_FAIL
from common.util.constant import ModelState
from common.util.constant import InferenceState
from common.util.monitor import init_monitor, run_monitor, stop_monitor

from common.tsanaclient import TSANAClient

import logging
from telemetry import log
from os import environ
import yaml

import atexit
from apscheduler.schedulers.background import BackgroundScheduler

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import asyncio

#async infras
#executor = ProcessPoolExecutor()
#ThreadPool easy for debug
executor = ThreadPoolExecutor()
loop = asyncio.new_event_loop()

#monitor infras
sched = BackgroundScheduler()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def load_config(path):
    try:
        with open(path, 'r') as config_file:
            config_yaml = yaml.safe_load(config_file)
            Config = namedtuple('Config', sorted(config_yaml))
            config = Config(**config_yaml)
        return config
    except Exception:
        return None


class PluginService():

    def __init__(self):
        config_file = environ.get('SERVICE_CONFIG_FILE')
        config = load_config(config_file)
        if config is None:
            log.error("No configuration '%s', or the configuration is not in JSON format. " % (config_file))
            exit()
        self.config = config
        self.tsanaclient = TSANAClient(config.tsana_api_endpoint, config.tsana_api_key, config.series_limit)

        init_monitor(config)
        sched.add_job(func=lambda: run_monitor(config), trigger="interval", seconds=10)
        sched.start()
        atexit.register(lambda: stop_monitor(config))
        atexit.register(lambda: sched.shutdown())

    def do_verify(self, subscription, parameters):
        return STATUS_SUCCESS, ''

    def do_train(self, subscription, model_id, model_dir, parameters):
        return STATUS_SUCCESS, ''

    def do_state(self, subscription, model_id):
        return STATUS_SUCCESS, ''

    def do_inference(self, subscription, model_id, model_dir, parameters):
        return STATUS_SUCCESS, ''

    def do_delete(self, subscription, model_id):
        return STATUS_SUCCESS, ''
        
    def train_wrapper(self, subscription, model_id, parameters, timekey, callback):
        log.info("Start train wrapper for model %s by %s " % (model_id, subscription))
        try:
            model_dir = os.path.join(self.config.model_temp_dir, subscription + '_' + model_id + '_' + str(timekey))
            os.makedirs(model_dir, exist_ok=True)
            result, message = self.do_train(subscription, model_id, model_dir, parameters)
            log.info("Train result, %s", message)
            
            if result == STATUS_SUCCESS:
                # in the callback, the model will be moved from temp dir to prd dir
                if callback is not None:
                    callback(subscription, model_id, parameters, ModelState.Ready, timekey, message)
            else:
                raise Exception(message)
        except Exception as e:
            if callback is not None:
                callback(subscription, model_id, parameters, ModelState.Failed, timekey, str(e))
        finally:
            shutil.rmtree(model_dir)
        return STATUS_SUCCESS, ''

    def get_inference_time_range(self, parameters):
        return []

    # inference_window: 30
    # endTime: endtime
    def inference_wrapper(self, subscription, model_id, parameters, timekey, callback): 
        log.info("Start inference wrapper %s by %s " % (model_id, subscription))
        try:
            results = [{'timestamp': timestamp, 'status': InferenceState.Running.name} for timestamp in self.get_inference_time_range(parameters)]
            self.tsanaclient.save_inference_result(parameters, results)

            prepare_model(self.config, subscription, model_id, timekey, True)
            prd_dir = os.path.join(self.config.model_temp_dir, subscription + '_' + model_id)
            result, message = self.do_inference(subscription, model_id, prd_dir, parameters)

            # TODO: Write the result back
            log.info("Inference result here: %s" % result)
            if callback is not None:
                callback(subscription, model_id, timekey, result)    
        except Exception as e:
            if callback is not None:
                callback(subscription, model_id, timekey, STATUS_FAIL, str(e))
        return STATUS_SUCCESS, ''

    def train_callback(self, subscription, model_id, parameters, model_state, timekey, last_error=None):
        log.info("Training callback %s by %s , state = %s" % (model_id, subscription, model_state))
        meta = get_meta(self.config, subscription, model_id)
        if meta is None or meta['state'] == ModelState.Deleted.name:
            return STATUS_FAIL, 'Model is not found! '  

        # Train finish, save the model and call callback
        if model_state == ModelState.Ready:
            result, message = copy_tree_and_zip_and_update_remote(self.config, subscription, model_id, timekey)
            if result != STATUS_SUCCESS:
                model_state = ModelState.Failed
                last_error = 'Model storage failed!'

        update_state(self.config, subscription, model_id, model_state, None, last_error)
        return self.tsanaclient.save_training_result(parameters, model_id, model_state, last_error)

    def inference_callback(self, subscription, model_id, parameters, timekey, result, last_error=None):
        log.info ("inference callback %s by %s , result = %s" % (model_id, subscription, result))
        if result == STATUS_FAIL: 
            # Inference failed
            # Do a model update
            prepare_model(self.config, subscription, model_id, timekey, True)
        
        return self.tsanaclient.save_inference_result(parameters, result['result'])

    def train(self, request):
        request_body = json.loads(request.data)
        instance_id = request_body['instance']['instanceId']
        subscription = request.headers.get('apim-subscription-id', 'Official')
        result, message = self.do_verify(subscription, request_body)
        if result != STATUS_SUCCESS:
            return make_response(jsonify(dict(instanceId=instance_id, modelId='', result=STATUS_FAIL, message='Verify failed! ' + message, modelState=ModelState.Deleted.name)), 400)

        models_in_train = []
        for model in get_model_list(self.config, subscription):
            if 'inst_id' in model and model['inst_id'] == request_body['instance']['instanceId'] and model['state'] == ModelState.Training.name:
                models_in_train.append(model['model_id'])

        if len(models_in_train) >= self.config.models_in_training_limit_per_instance:
            return make_response(jsonify(dict(instanceId=instance_id, modelId='', result=STATUS_FAIL, message='Models in training limit reached! Abort training this time.', modelState=ModelState.Deleted.name)), 400)

        log.info('Create training task')
        try:
            model_id = str(uuid.uuid1())
            insert_meta(self.config, subscription, model_id, request_body)
            meta = get_meta(self.config, subscription, model_id)
            timekey = meta['timekey']
            asyncio.ensure_future(loop.run_in_executor(executor, self.train_wrapper, subscription, model_id, request_body, timekey, self.train_callback))
            return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, result=STATUS_SUCCESS, message='Training task created', modelState=ModelState.Training.name)), 201)
        except Exception as e: 
            meta = get_meta(self.config, subscription, model_id)
            if meta is not None and meta['timekey'] == timekey: 
                update_state(self.config, subscription, model_id, ModelState.Failed, None, str(e))
            return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, result=STATUS_FAIL, message='Fail to create new task ' + str(e), modelState=ModelState.Failed.name)), 400)

    def inference(self, request, model_id):
        request_body = json.loads(request.data)
        instance_id = request_body['instance']['instanceId']
        subscription = request.headers.get('apim-subscription-id', 'Official')
        result, message = self.do_verify(subscription, request_body)
        if result != STATUS_SUCCESS:
            return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, result=STATUS_FAIL, message='Verify failed! ' + message, modelState=ModelState.Failed.name)), 400)

        meta = get_meta(self.config, subscription, model_id)
        if meta is None:
            return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, result=STATUS_FAIL, message='Model is not found!', modelState=ModelState.Deleted.name)), 400)
            
        if meta['state'] != ModelState.Ready.name:
            return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, result=STATUS_FAIL, message='Cannot do inference right now, status is ' + meta['state'], modelState=meta['state'])), 400)

        current_set = meta['series_set']
        current_para = meta['para']

        new_set = str(request_body['seriesSets'])
        new_para = str(request_body['instance']['params'])

        if current_set != new_set or current_para != new_para:
            return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, result=STATUS_FAIL, message='Inconsistent series sets or params!', modelState=meta['state'])), 400)

        log.info('Create inference task')
        timekey = meta['timekey']
        asyncio.ensure_future(loop.run_in_executor(executor, self.inference_wrapper, subscription, model_id, request_body, timekey, self.inference_callback))
        return make_response(jsonify(dict(instanceId=instance_id, modelId=model_id, result=STATUS_SUCCESS, message='Inference task created', modelState=meta['state'])), 201)

    def state(self, request, model_id):
        try:
            subscription = request.headers.get('apim-subscription-id', 'Official')
            meta = get_meta(self.config, subscription, model_id)
            if meta == None:
                return make_response(jsonify(dict(instanceId='', modelId=model_id, result=STATUS_FAIL, message='Model is not found!', modelState=ModelState.Deleted.name)), 400)

            meta = clear_state_when_necessary(self.config, subscription, model_id, meta)
            return make_response(jsonify(dict(instanceId='', modelId=model_id, result=STATUS_SUCCESS, message='', modelState=meta['state'])), 200)
        except Exception as e:
            return make_response(jsonify(dict(instanceId='', modelId=model_id, result=STATUS_FAIL, message=str(e), modelState=ModelState.Failed.name)), 400)
        
    def list_models(self, request):
        subscription = request.headers.get('apim-subscription-id', 'Official')
        return make_response(jsonify(get_model_list(self.config, subscription)), 200)

    def delete(self, request, model_id):
        try:
            subscription = request.headers.get('apim-subscription-id', 'Official')
            result, message = self.do_delete(subscription, model_id)
            if result == STATUS_SUCCESS:
                update_state(self.config, subscription, model_id, ModelState.Deleted)
                return make_response(jsonify(dict(instanceId='', modelId=model_id, result=STATUS_SUCCESS, message='Model {} has been deleted'.format(model_id), modelState=ModelState.Deleted.name)), 200)
            else:
                raise Exception(message)
        except Exception as e:
            return make_response(jsonify(dict(instanceId='', modelId=model_id, result=STATUS_FAIL, message=str(e), modelState=ModelState.Failed.name)), 400)

    def verify(self, request):
        request_body = json.loads(request.data)
        instance_id = request_body['instance']['instanceId']
        subscription = request.headers.get('apim-subscription-id', 'Official')
        result, message = self.do_verify(subscription, request_body)
        if result != STATUS_SUCCESS:
            return make_response(jsonify(dict(instanceId=instance_id, modelId='', result=STATUS_FAIL, message='Verify failed! ' + message, modelState=ModelState.Deleted.name)), 400)
        else:
            return make_response(jsonify(dict(instanceId=instance_id, modelId='', result=STATUS_SUCCESS, message='Verify successfully! ' + message, modelState=ModelState.Deleted.name)), 200)