import json
import time
from time import gmtime, strftime
import os
import shutil
from flask import jsonify, make_response
from collections import namedtuple
import uuid

from common.util.timeutil import get_time_offset, str_to_dt, dt_to_str, get_time_list
from common.util.data import get_metric_meta, do_verify, get_timeseries, upload_data
from common.util.meta import insert_meta, get_meta, update_state, get_model_list
from common.util.model import copy_tree_and_zip_and_update_remote, prepare_model
from common.util.constant import STATUS_SUCCESS, STATUS_FAIL
from common.util.constant import ModelState
from common.util.constant import InferenceState
from common.util.monitor import init_monitor, run_monitor, stop_monitor

from common.tsanaclient import TSANAClient

from telemetry import log
from os import environ
import yaml

import atexit
from apscheduler.schedulers.background import BackgroundScheduler

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import asyncio

#async infras
executor = ProcessPoolExecutor()
#ThreadPool easy for debug
#executor = ThreadPoolExecutor()
loop = asyncio.new_event_loop()

#monitor infras
sched = BackgroundScheduler()

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

    def do_train(self, subscription, model_key, model_dir, parameters):
        return STATUS_SUCCESS, ''

    def do_state(self, subscription, model_key):
        return STATUS_SUCCESS, ''

    def do_inference(self, subscription, model_key, model_dir, parameters):
        return STATUS_SUCCESS, ''

    def do_delete(self, subscription, model_key):
        return STATUS_SUCCESS, ''
        
    def train_wrapper(self, subscription, model_key, parameters, timekey, callback):
        log.info("Start train wrapper for model %s by %s ", model_key, subscription)
        try:
            model_dir = os.path.join(self.config.model_temp_dir, subscription + '_' + model_key + '_' + str(timekey))
            os.makedirs(model_dir, exist_ok=True)
            result, message = self.do_train(subscription, model_key, model_dir, parameters)
            log.info("Train result, %s", message)
            # in the callback, the model will be moved from temp dir to prd dir
            if callback is not None:
                if result == STATUS_SUCCESS:
                    callback(subscription, model_key, ModelState.READY, timekey, '')
                else:
                    callback(subscription, model_key, ModelState.FAILED, timekey, message)
        except Exception as e:
            if callback is not None:
                callback(subscription, model_key, ModelState.FAILED, str(e))
        finally:
            shutil.rmtree(model_dir)
        return STATUS_SUCCESS, ''

    # inference_window: 30
    # endTime: endtime
    def inference_wrapper(self, subscription, model_key, parameters, timekey, callback): 
        log.info("Start inference wrapper %s by %s ", model_key, subscription)
        try:
            prepare_model(self.config, subscription, model_key, timekey, True)
            prd_dir = os.path.join(self.config.model_temp_dir, subscription + '_' + model_key)
            result, message = self.do_inference(subscription, model_key, prd_dir, parameters)

            # TODO: Write the result back
            log.info("Inference result here: %s" % result)
            if callback is not None:
                callback(subscription, model_key, timekey, result)    
        except Exception as e:
            if callback is not None:
                callback(subscription, model_key, timekey, STATUS_FAIL, str(e))
        return STATUS_SUCCESS, ''

    def train_callback(self, subscription, model_key, model_state, timekey, last_error=''): 
        log.info("Training callback %s by %s , state = %s", model_key, subscription, model_state)
        meta = get_meta(self.config, subscription, model_key)
        if meta is None or meta['state'] == ModelState.DELETED.name:
            return STATUS_FAIL, 'Model is not found! '  

        # Train finish, save the model and call callback
        if model_state == ModelState.READY:
            result, message = copy_tree_and_zip_and_update_remote(self.config, subscription, model_key, timekey)
            if result != STATUS_SUCCESS:
                model_state = ModelState.FAILED
                last_error = 'Model storage failed!'

        return update_state(self.config, subscription, model_key, model_state.name, last_error)

    def inference_callback(self, subscription, model_key, timekey, result, last_error=''):
        log.info ("inference callback %s by %s , result = %s", model_key, subscription, result)
        if result == STATUS_FAIL: 
            # Inference failed
            # Do a model update
            prepare_model(self.config, subscription, model_key, timekey, True)

    def train(self, request):
        request_body = json.loads(request.data)
        subscription = request.headers.get('apim-subscription-id', 'Official')
        result, message = self.do_verify(subscription, request_body)
        if result != STATUS_SUCCESS:
            return make_response(jsonify(dict(status=STATUS_FAIL, message='Verify failed! ' + message)), 400)

        models_in_train = [model for model in get_model_list(self.config, subscription) if model['state'] == ModelState.TRAINING.name]
        if len(models_in_train) >= self.config.models_in_training_limit:
            return make_response(jsonify(dict(status=STATUS_FAIL, message='Models in training limit reached! Abort training this time.')), 400)

        log.info('Create training task')
        try:
            model_key = str(uuid.uuid1())
            insert_meta(self.config, subscription, model_key, request_body)
            meta = get_meta(self.config, subscription, model_key)
            timekey = meta['timekey']
            asyncio.ensure_future(loop.run_in_executor(executor, self.train_wrapper, subscription, model_key, request_body, timekey, self.train_callback))
            return make_response(jsonify(dict(status=STATUS_SUCCESS, model_key=model_key, message='Training task created')), 200)
        except Exception as e: 
            meta = get_meta(self.config, subscription, model_key)
            if meta is not None and meta['timekey'] == timekey: 
                update_state(self.config, subscription, model_key, ModelState.FAILED, str(e))
            return make_response(jsonify(dict(status=STATUS_FAIL, message='Fail to create new task ' + str(e))), 400)

    def inference(self, request, model_key):
        request_body = json.loads(request.data)
        subscription = request.headers.get('apim-subscription-id', 'Official')
        result, message = self.do_verify(subscription, request_body)
        if result != STATUS_SUCCESS:
            return make_response(jsonify(dict(status=STATUS_FAIL, message='Verify failed! ' + message)), 400)

        meta = get_meta(self.config, subscription, model_key)
        if meta['state'] != ModelState.READY.name:
            return STATUS_FAIL, 'Cannot do inference right now, status is ' + meta['state']

        log.info('Create inference task')
        timekey = meta['timekey']  
        asyncio.ensure_future(loop.run_in_executor(executor, self.inference_wrapper, subscription, model_key, request_body, timekey, self.inference_callback))
        return make_response(jsonify(dict(status=STATUS_SUCCESS, message='Inference task created')), 200)

    def state(self, request, model_key):
        subscription = request.headers.get('apim-subscription-id', 'Official')
        meta = get_meta(self.config, subscription, model_key)
        if meta == None:
            return make_response(jsonify(dict(status=STATUS_FAIL, message='Model is not found! ')), 400)

        return make_response(jsonify(dict(meta['state'])), 200)

    def list_models(self, request):
        subscription = request.headers.get('apim-subscription-id', 'Official')
        return make_response(jsonify(get_model_list(self.config, subscription)), 200)

    def delete(self, request, model_key):
        subscription = request.headers.get('apim-subscription-id', 'Official')
        result, message = self.do_delete(subscription, model_key)
        if result == STATUS_SUCCESS:
            update_state(self.config, subscription, model_key, ModelState.DELETED)
            return make_response(jsonify(dict(status=STATUS_SUCCESS, model_key=model_key)), 200)
        else:
            return make_response(jsonify(dict(status=STATUS_FAIL, message=message)), 400)