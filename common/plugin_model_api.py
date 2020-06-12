import time
from telemetry import log

from flask import Flask, request, g, jsonify
from flask_restful import Resource, Api

from .plugin_service import PluginService
from .util.constant import STATUS_SUCCESS, STATUS_FAIL

app = Flask(__name__)
api = Api(app)


def try_except(fn):
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            log.error("-----Exception-----")
            return jsonify(dict(status=STATUS_FAIL, message='Unknown error, please check your request. ' + str(e))), 502
    return wrapped

@app.route('/', methods=['GET'])
def index():
    return "Welcome to TSANA Computing Platform"


@app.before_request
def before_request():
    g.start = time.time()


@app.after_request
def after_request(response):
    total_time = (time.time() - g.start) * 1e6
    rule = str(request.url_rule)
    status = str(response.status_code)
    # TODO log here
    return response


class PluginModelAPI(Resource):  # The API class that handles a single user
    def __init__(self, plugin_service: PluginService):
        self.__plugin_service = plugin_service

    @try_except
    def get(self, model_key):
        return self.__plugin_service.state(request, model_key)

    @try_except
    def put(self, model_key):
        pass

    @try_except
    def delete(self, model_key):
        return self.__plugin_service.delete(request, model_key)

    @try_except
    def post(self):
        return self.__plugin_service.create(request)

    @try_except
    def patch(self, model_key):
        return self.__plugin_service.update(request, model_key)


class PluginModelTrainAPI(Resource):
    def __init__(self, plugin_service: PluginService):
        self.__plugin_service = plugin_service

    @try_except
    def post(self, model_key):
        return self.__plugin_service.train(request, model_key)


class PluginModelInferenceAPI(Resource):
    def __init__(self, plugin_service: PluginService):
        self.__plugin_service = plugin_service

    @try_except
    def post(self, model_key):
        return self.__plugin_service.inference(request, model_key)


class PluginModelParameterAPI(Resource):
    def __init__(self, plugin_service: PluginService):
        self.__plugin_service = plugin_service

    @try_except
    def post(self, model_key):
        return self.__plugin_service.verify(request, model_key)


class PluginModelListAPI(Resource):
    def __init__(self, plugin_service: PluginService):
        self.__plugin_service = plugin_service

    @try_except
    def get(self):
        return self.__plugin_service.list_models(request)