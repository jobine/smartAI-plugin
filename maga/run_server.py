from os import environ

from maga.maga_plugin_service import MagaPluginService
from common.plugin_model_api import api, PluginModelAPI, PluginModelListAPI, PluginModelTrainAPI, \
    PluginModelInferenceAPI, app, PluginModelParameterAPI

multivarite = MagaPluginService()

api.add_resource(PluginModelListAPI(multivarite), '/multivarite/models')
api.add_resource(PluginModelAPI(multivarite), '/multivarite/model', '/multivarite/model/<model_key>')
api.add_resource(PluginModelTrainAPI(multivarite), '/multivarite/<model_key>/train')
api.add_resource(PluginModelInferenceAPI(multivarite), '/multivarite/<model_key>/inference')
api.add_resource(PluginModelParameterAPI(multivarite), '/multivarite/parameters')

if __name__ == '__main__':
    HOST = environ.get('SERVER_HOST', '0.0.0.0')
    PORT = environ.get('SERVER_PORT', 56789)
    app.run(HOST, PORT, threaded=True, use_reloader=False)