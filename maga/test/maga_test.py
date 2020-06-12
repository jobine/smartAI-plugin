from requests import Request
from maga.maga_plugin_service import MagaPluginService
from os import environ
import json

if __name__ == '__main__':    

    environ['SERVICE_CONFIG_FILE'] = 'maga/config/service_config.yaml'

    maga_plugin = MagaPluginService()
    request = Request()
    request.data = r'{"groupId":"8e826a5d-1b01-4ff4-a699-38bea97e17de","seriesSets":[{"seriesSetId":"b643e346-6883-4764-84a5-e63a3788eec9","metricId":"dc5b66cf-6dd0-4c83-bb8f-d849e68a7660","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_high","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"high","datafeedName":"Stock price","dataStartFrom":1105315200000}},{"seriesSetId":"0d4cce4d-f4d4-4cef-be87-dbd28062abfc","metricId":"3274f7e6-683b-4d92-b134-0c1186e416a1","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_change","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"change","datafeedName":"Stock price","dataStartFrom":1105315200000}}],"gran":{"granularityString":"Daily","customInSeconds":0},"instance":{"instanceName":"Maga_Instance_1586447708033","instanceId":"528cbe52-cb6a-44c0-b388-580aba57f2f7","status":"Active","appId":"173276d9-a7ed-494b-9300-6dd1aa09f2c3","appName":"Maga","appDisplayName":"Maga","appType":"Internal","remoteModelKey":"","params":{"fillUpMode":"Outer","tracebackWindow":28}},"startTime":"2020-03-05T00:00:00Z","endTime":"2020-05-18T00:00:00Z"}'

    response = maga_plugin.train(request)
    print(response)

    response = { 'modelId' : '1799535a-aa15-11ea-82db-0242ac110002'}
    status = maga_plugin.state(request, response['modelId'])
    print(status)
    
    inference_result = maga_plugin.inference_wrapper(request, response['modelId'], maga_plugin.prepare_inference_data(json.loads(request.data)), maga_plugin.inference_callback)
    print(inference_result)

    models = maga_plugin.list_models(request)
    print(models)
    
    delete_result = maga_plugin.delete(request, response['modelId'])
    print(delete_result)