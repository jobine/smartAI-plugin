import os
import sys
from os import environ

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))

environ['SERVICE_CONFIG_FILE'] = 'sample/demo_modeless/config/service_config.yaml'

from sample.demo_modeless.demo_service import DemoService
from common.plugin_model_api import api, PluginModelAPI, PluginModelListAPI, PluginModelTrainAPI, PluginModelInferenceAPI, app, PluginModelParameterAPI

demo = DemoService()

api.add_resource(PluginModelListAPI, '/demomodeless/models', resource_class_kwargs={'plugin_service': demo})
api.add_resource(PluginModelAPI, '/demomodeless/models/<model_id>', resource_class_kwargs={'plugin_service': demo})
api.add_resource(PluginModelTrainAPI, '/demomodeless/models/train', resource_class_kwargs={'plugin_service': demo})
api.add_resource(PluginModelInferenceAPI, '/demomodeless/models/<model_id>/inference', resource_class_kwargs={'plugin_service': demo})
api.add_resource(PluginModelParameterAPI, '/demomodeless/parameters', resource_class_kwargs={'plugin_service': demo})

if __name__ == '__main__':
    HOST = environ.get('SERVER_HOST', '0.0.0.0')
    PORT = environ.get('SERVER_PORT', 56789)
    app.run(HOST, PORT, threaded=True, use_reloader=False)