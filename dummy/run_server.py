import os
import sys
from os import environ

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))

environ['SERVICE_CONFIG_FILE'] = 'dummy/config/service_config.yaml'

from dummy.dummy_plugin_service import DummyPluginService
from common.plugin_model_api import api, PluginModelAPI, PluginModelListAPI, PluginModelTrainAPI, \
    PluginModelInferenceAPI, app, PluginModelParameterAPI

dummy = DummyPluginService()

api.add_resource(PluginModelListAPI, '/dummy/models', resource_class_kwargs={'plugin_service': dummy})
api.add_resource(PluginModelAPI, '/dummy/models/<model_id>', resource_class_kwargs={'plugin_service': dummy})
api.add_resource(PluginModelTrainAPI, '/dummy/models/train', resource_class_kwargs={'plugin_service': dummy})
api.add_resource(PluginModelInferenceAPI, '/dummy/models/<model_id>/inference', resource_class_kwargs={'plugin_service': dummy})
api.add_resource(PluginModelParameterAPI, '/dummy/parameters', resource_class_kwargs={'plugin_service': dummy})

if __name__ == '__main__':
    HOST = environ.get('SERVER_HOST', '0.0.0.0')
    PORT = environ.get('SERVER_PORT', 56789)
    app.run(HOST, PORT, threaded=True, use_reloader=False)