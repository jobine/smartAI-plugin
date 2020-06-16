
# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

# Usage

## Use Maga plugin
    cd smartAI-plugin 
    docker build -f maga-plugin.Dockerfile -t maga:latest .
    docker run -p 56789:56789 maga
## Use Forecast plugin
    cd smartAI-plugin 
    docker build -f forecast-plugin.Dockerfile -t forecast:latest .
    docker run -p 56789:56789 forecast
## Implement your own plugin
    Inherit PluginService in commone and implement do_verify, do_train, do_state, do_inference, do_delete, etc. if needed.
    Add apis to run_server, bind restful apis with your plugin.
    Add dockerfile for your plugin.

# API (MAGA)
## train
### URL: /multivariate/models/train
### Method: **POST**
### URL Params:
### Data Params(json):
#### {"groupId":"8e826a5d-1b01-4ff4-a699-38bea97e17de","seriesSets":[{"seriesSetId":"b643e346-6883-4764-84a5-e63a3788eec9","metricId":"dc5b66cf-6dd0-4c83-bb8f-d849e68a7660","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_high","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"high","datafeedName":"Stock price","dataStartFrom":1105315200000}},{"seriesSetId":"0d4cce4d-f4d4-4cef-be87-dbd28062abfc","metricId":"3274f7e6-683b-4d92-b134-0c1186e416a1","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_change","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"change","datafeedName":"Stock price","dataStartFrom":1105315200000}}],"gran":{"granularityString":"Daily","customInSeconds":0},"instance":{"instanceName":"Maga_Instance_1586447708033","instanceId":"528cbe52-cb6a-44c0-b388-580aba57f2f8","status":"Active","appId":"173276d9-a7ed-494b-9300-6dd1aa09f2c3","appName":"Maga","appDisplayName":"Maga","appType":"Internal","remoteModelKey":"","params":{"fillUpMode":"Outer","tracebackWindow":28}},"startTime":"2020-03-05T00:00:00Z","endTime":"2020-05-18T00:00:00Z"}
### Response: 
#### {"errorCode": "","errorMessage": "","modelId": "629027d4-afbb-11ea-b6b1-f24275a4cb64"}

## state
### URL: /multivariate/models/<*modleId*>
### Method: **GET**
### URL Params: modelId=[string]
### Data Params: None
### Response:
#### {"errorCode":"","errorMessage":"","modelId":"629027d4-afbb-11ea-b6b1-f24275a4cb64","status":"READY","createdDateTime":"2020-06-16T10:23:13Z","lastUpdatedDateTime":"2020-06-16T10:23:13Z","lossMessage":["| end of epoch  98/100 | time:  0.04s | train_loss 6.4319 | train_pre 0.0046 | train_kl 6.3848 |train_vae 0.0425 | valid total 5.5332 | valid pre 0.0274 | valid vae  5.5058","| end of epoch  99/100 | time:  0.03s | train_loss 6.6124 | train_pre 0.0030 | train_kl 6.5946 |train_vae 0.0148 | valid total 5.6492 | valid pre 0.0274 | valid vae  5.6218","| end of epoch 100/100 | time:  0.04s | train_loss 8.8963 | train_pre 0.0036 | train_kl 8.8875 |train_vae 0.0052 | valid total 5.9264 | valid pre 0.0275 | valid vae  5.8990"],"missingRatio":{"85b617208b0f90ed1e5219d8bcd34469":0.0,"8746c8e61221c8061579456952a71dc0":0.0}}

## inference
### URL: /multivariate/models/<*modleId*>/inference
### Method: **POST**
### URL Params: modelId=[string]
### Data Params:
#### {"groupId":"8e826a5d-1b01-4ff4-a699-38bea97e17de","seriesSets":[{"seriesSetId":"b643e346-6883-4764-84a5-e63a3788eec9","metricId":"dc5b66cf-6dd0-4c83-bb8f-d849e68a7660","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_high","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"high","datafeedName":"Stock price","dataStartFrom":1105315200000}},{"seriesSetId":"0d4cce4d-f4d4-4cef-be87-dbd28062abfc","metricId":"3274f7e6-683b-4d92-b134-0c1186e416a1","dimensionFilter":{"ts_code":"600030.SH"},"seriesSetName":"Stock price_change","metricMeta":{"granularityName":"Daily","granularityAmount":0,"datafeedId":"29595b1c-531f-445c-adcf-b75b2ab93c34","metricName":"change","datafeedName":"Stock price","dataStartFrom":1105315200000}}],"gran":{"granularityString":"Daily","customInSeconds":0},"instance":{"instanceName":"Maga_Instance_1586447708033","instanceId":"528cbe52-cb6a-44c0-b388-580aba57f2f8","status":"Active","appId":"173276d9-a7ed-494b-9300-6dd1aa09f2c3","appName":"Maga","appDisplayName":"Maga","appType":"Internal","remoteModelKey":"","params":{"fillUpMode":"Outer","tracebackWindow":28}},"startTime":"2020-03-05T00:00:00Z","endTime":"2020-05-18T00:00:00Z"}
### Response: 
#### {"message":"Inference task created","status":"Success"}
#### Result will write back to TSANA in inference_callback by default.

## delete
### URL: /multivariate/models/<*modleId*>
### Method: **DELETE**
### URL Params: modelId=[string]
### Data Params: None
### Response:
#### {"errorCode":"","errorMessage":""}

## list_models
### URL: /multivariate/models
### Method: **GET**
### URL Params: None
### Data Params: None
### Response:
#### {"models":[{"errorCode":"","errorMessage":"","modelId":"206114bc-ac7e-11ea-a493-f24275a4cb64","status":"READY","createdDateTime":"2020-06-12T07:27:09Z","lastUpdatedDateTime":"2020-06-12T07:27:09Z","lossMessage":["| end of epoch  98/100 | time:  0.03s | train_loss 6.4319 | train_pre 0.0046 | train_kl 6.3848 |train_vae 0.0425 | valid total 5.5332 | valid pre 0.0274 | valid vae  5.5058","| end of epoch  99/100 | time:  0.03s | train_loss 6.6124 | train_pre 0.0030 | train_kl 6.5946 |train_vae 0.0148 | valid total 5.6492 | valid pre 0.0274 | valid vae  5.6218","| end of epoch 100/100 | time:  0.03s | train_loss 8.8963 | train_pre 0.0036 | train_kl 8.8875 |train_vae 0.0052 | valid total 5.9264 | valid pre 0.0275 | valid vae  5.8990"],"missingRatio":{"85b617208b0f90ed1e5219d8bcd34469":0.0,"8746c8e61221c8061579456952a71dc0":0.0}},{"errorCode":"","errorMessage":"","modelId":"595f02c4-ac7e-11ea-b216-f24275a4cb64","status":"READY","createdDateTime":"2020-06-12T07:28:45Z","lastUpdatedDateTime":"2020-06-12T07:28:45Z","lossMessage":["| end of epoch  98/100 | time:  0.03s | train_loss 6.4319 | train_pre 0.0046 | train_kl 6.3848 |train_vae 0.0425 | valid total 5.5332 | valid pre 0.0274 | valid vae  5.5058","| end of epoch  99/100 | time:  0.04s | train_loss 6.6124 | train_pre 0.0030 | train_kl 6.5946 |train_vae 0.0148 | valid total 5.6492 | valid pre 0.0274 | valid vae  5.6218","| end of epoch 100/100 | time:  0.04s | train_loss 8.8963 | train_pre 0.0036 | train_kl 8.8875 |train_vae 0.0052 | valid total 5.9264 | valid pre 0.0275 | valid vae  5.8990"],"missingRatio":{"85b617208b0f90ed1e5219d8bcd34469":0.0,"8746c8e61221c8061579456952a71dc0":0.0}},{"errorCode":"","errorMessage":"","modelId":"dab84214-ac7d-11ea-8938-f24275a4cb64","status":"READY","createdDateTime":"2020-06-12T07:25:13Z","lastUpdatedDateTime":"2020-06-12T07:25:13Z","lossMessage":["| end of epoch  98/100 | time:  0.04s | train_loss 6.4319 | train_pre 0.0046 | train_kl 6.3848 |train_vae 0.0425 | valid total 5.5332 | valid pre 0.0274 | valid vae  5.5058","| end of epoch  99/100 | time:  0.03s | train_loss 6.6124 | train_pre 0.0030 | train_kl 6.5946 |train_vae 0.0148 | valid total 5.6492 | valid pre 0.0274 | valid vae  5.6218","| end of epoch 100/100 | time:  0.04s | train_loss 8.8963 | train_pre 0.0036 | train_kl 8.8875 |train_vae 0.0052 | valid total 5.9264 | valid pre 0.0275 | valid vae  5.8990"],"missingRatio":{"85b617208b0f90ed1e5219d8bcd34469":0.0,"8746c8e61221c8061579456952a71dc0":0.0}},{"errorCode":"","errorMessage":"","modelId":"f3e4c456-ac7d-11ea-92d9-f24275a4cb64","status":"READY","createdDateTime":"2020-06-12T07:25:55Z","lastUpdatedDateTime":"2020-06-12T07:25:55Z","lossMessage":["| end of epoch  98/100 | time:  0.04s | train_loss 6.4319 | train_pre 0.0046 | train_kl 6.3848 |train_vae 0.0425 | valid total 5.5332 | valid pre 0.0274 | valid vae  5.5058","| end of epoch  99/100 | time:  0.03s | train_loss 6.6124 | train_pre 0.0030 | train_kl 6.5946 |train_vae 0.0148 | valid total 5.6492 | valid pre 0.0274 | valid vae  5.6218","| end of epoch 100/100 | time:  0.04s | train_loss 8.8963 | train_pre 0.0036 | train_kl 8.8875 |train_vae 0.0052 | valid total 5.9264 | valid pre 0.0275 | valid vae  5.8990"],"missingRatio":{"85b617208b0f90ed1e5219d8bcd34469":0.0,"8746c8e61221c8061579456952a71dc0":0.0}}],"errorCode":"","errorMessage":""}
