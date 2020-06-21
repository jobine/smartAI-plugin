import os
import sys
from os import environ

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))

environ['SERVICE_CONFIG_FILE'] = 'maga/config/service_config.yaml'
from maga.maga_plugin_service import MagaPluginService
from common.plugin_model_api import api, PluginModelAPI, PluginModelListAPI, PluginModelTrainAPI, \
    PluginModelInferenceAPI, app, PluginModelParameterAPI

multivariate = MagaPluginService()

api.add_resource(PluginModelListAPI, '/multivariate/models', resource_class_kwargs={'plugin_service': multivariate})
api.add_resource(PluginModelAPI, '/multivariate/models/<model_id>', resource_class_kwargs={'plugin_service': multivariate})
api.add_resource(PluginModelTrainAPI, '/multivariate/models/train', resource_class_kwargs={'plugin_service': multivariate})
api.add_resource(PluginModelInferenceAPI, '/multivariate/models/<model_id>/inference', resource_class_kwargs={'plugin_service': multivariate})
api.add_resource(PluginModelParameterAPI, '/multivariate/parameters', resource_class_kwargs={'plugin_service': multivariate})

if __name__ == '__main__':
    HOST = environ.get('SERVER_HOST', '0.0.0.0')
    PORT = environ.get('SERVER_PORT', 56789)
    app.run(HOST, PORT, threaded=True, use_reloader=False)