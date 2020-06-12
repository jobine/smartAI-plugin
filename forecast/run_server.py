from os import environ

from forecast.forecast_plugin_service import ForecastPluginService
from common.plugin_model_api import api, PluginModelAPI, PluginModelListAPI, PluginModelTrainAPI, \
    PluginModelInferenceAPI, app, PluginModelParameterAPI

forecast = ForecastPluginService()

api.add_resource(PluginModelListAPI(forecast), '/forecast/models')
api.add_resource(PluginModelAPI(forecast), '/forecast/model', '/forecast/model/<model_key>')
api.add_resource(PluginModelTrainAPI(forecast), '/forecast/<model_key>/train')
api.add_resource(PluginModelInferenceAPI(forecast), '/forecast/<model_key>/inference')
api.add_resource(PluginModelParameterAPI(forecast), '/forecast/parameters')

if __name__ == '__main__':
    HOST = environ.get('SERVER_HOST', '0.0.0.0')
    PORT = environ.get('SERVER_PORT', 56789)
    app.run(HOST, PORT, threaded=True, use_reloader=False)
