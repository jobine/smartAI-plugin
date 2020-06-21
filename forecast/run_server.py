import os
import sys
from os import environ

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))

environ['SERVICE_CONFIG_FILE'] = 'forecast/config/service_config.yaml'

from forecast.forecast_plugin_service import ForecastPluginService
from common.plugin_model_api import api, PluginModelAPI, PluginModelListAPI, PluginModelTrainAPI, \
    PluginModelInferenceAPI, app, PluginModelParameterAPI

forecast = ForecastPluginService()

api.add_resource(PluginModelListAPI, '/forecast/models', resource_class_kwargs={'plugin_service': forecast})
api.add_resource(PluginModelAPI, '/forecast/models/<model_id>', resource_class_kwargs={'plugin_service': forecast})
api.add_resource(PluginModelTrainAPI, '/forecast/models/train', resource_class_kwargs={'plugin_service': forecast})
api.add_resource(PluginModelInferenceAPI, '/forecast/models/<model_id>/inference', resource_class_kwargs={'plugin_service': forecast})
api.add_resource(PluginModelParameterAPI, '/forecast/parameters', resource_class_kwargs={'plugin_service': forecast})

if __name__ == '__main__':
    HOST = environ.get('SERVER_HOST', '0.0.0.0')
    PORT = environ.get('SERVER_PORT', 56789)
    app.run(HOST, PORT, threaded=True, use_reloader=False)