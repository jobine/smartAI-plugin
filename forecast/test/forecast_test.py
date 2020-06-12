from requests import Request
from forecast.forecast_plugin_service import ForecastPluginService
from os import environ
import json

if __name__ == '__main__':    

    environ['SERVICE_CONFIG_FILE'] = 'forecast/config/service_config.yaml'

    forecast_plugin = ForecastPluginService()
    request = Request()
    request.data = r'{"groupId":"8e826a5d-1b01-4ff4-a699-38bea97e17de","seriesSets":[{"seriesSetId":"b643e346-6883-4764-84a5-e63a3788eec9","metricId":"dc5b66cf-6dd0-4c83-bb8f-d849e68a7660","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_high","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"high","datafeedName":"Stock price","dataStartFrom":1105315200000}},{"seriesSetId":"0d4cce4d-f4d4-4cef-be87-dbd28062abfc","metricId":"3274f7e6-683b-4d92-b134-0c1186e416a1","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_change","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"change","datafeedName":"Stock price","dataStartFrom":1105315200000}}],"gran":{"granularityString":"Daily","customInSeconds":0},"instance":{"instanceName":"Forecast_Instance_1586447708033","instanceId":"528cbe52-cb6a-44c0-b388-580aba57f2f7","status":"Active","appId":"173276d9-a7ed-494b-9300-6dd1aa09f2c3","appName":"Forecast","appDisplayName":"Forecast","appType":"Internal","remoteModelKey":"","params":{"missingRatio":0.5,"target":{"filters":{"ts_code":"600030.SH"},"metricId":"dc5b66cf-6dd0-4c83-bb8f-d849e68a7660","name":"Stock price_high"},"waitInSeconds":60,"windowSize":28, "step":1},"hookIds":[]},"startTime":"2020-03-18T00:00:00Z","endTime":"2020-04-18T00:00:00Z","modelId":""}'

    #response = forecast_plugin.train(request)
    #print(response)

    response = { 'modelId' : 'a735a3be-abfc-11ea-a803-000d3af88183'}
    #status = forecast_plugin.state(request, response['modelId'])
    #print(status)
    
    inference_result = forecast_plugin.inference(request, response['modelId'])
    print(inference_result)

    models = forecast_plugin.list_models(request)
    print(models)
    
    delete_result = forecast_plugin.delete(request, response['modelId'])
    print(delete_result)