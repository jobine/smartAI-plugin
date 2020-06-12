import json

from common.util.timeutil import dt_to_str
from common.util.retryrequests import RetryRequests

REQUEST_TIMEOUT_SECONDS = 30

class MAGAClient(object):
    def __init__(self, endpoint, username=None, password=None, retrycount=3, retryinterval=1000):
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.retrycount = retrycount
        self.retryinterval = retryinterval

    def post(self, path, data, subscription):
        url = self.endpoint + path
        headers = {'Content-Type': 'application/json', 'apim-subscription-id': subscription}
        if self.username and self.password:
            auth = (self.username, self.password)
        else:
            auth = None
        retryrequests = RetryRequests(self.retrycount, self.retryinterval)
        try:
            r = retryrequests.post(url=url, headers=headers, auth=auth, data=json.dumps(data),
                                   timeout=REQUEST_TIMEOUT_SECONDS, verify=False)
            return r.json()
        except Exception as e:
            raise Exception('MAGA service api "{}" failed, request:{}, {}'.format(path, data, str(e)))

    def get(self, path, subscription):
        url = self.endpoint + path
        headers = {'Content-Type': 'application/json', 'apim-subscription-id': subscription}
        if self.username and self.password:
            auth = (self.username, self.password)
        else:
            auth = None
        retryrequests = RetryRequests(self.retrycount, self.retryinterval)
        try:
            r = retryrequests.get(url=url, headers=headers, auth=auth, timeout=REQUEST_TIMEOUT_SECONDS, verify=False)
            return r.json()
        except Exception as e:
            raise Exception('MAGA service api "{}" failed, {}'.format(path, str(e)))

    def delete(self, path, subscription):
        url = self.endpoint + path
        headers = {'Content-Type': 'application/json', 'apim-subscription-id': subscription}
        if self.username and self.password:
            auth = (self.username, self.password)
        else:
            auth = None
        retryrequests = RetryRequests(self.retrycount, self.retryinterval)
        try:
            r = retryrequests.delete(url=url, headers=headers, auth=auth, timeout=REQUEST_TIMEOUT_SECONDS, verify=False)
            return r.json()
        except Exception as e:
            raise Exception('MAGA service api "{}" failed, {}'.format(path, str(e)))

    def train(self, request):
        return self.post('/multivariate/models', request.data, request.headers.get('apim-subscription-id', 'Official'))

    def inference(self, request, model_key):
        return self.post('/multivariate/models/' + model_key + '/detect', request.data, request.headers.get('apim-subscription-id', 'Official'))

    def state(self, request, model_key):
        return self.get('/multivariate/models/' + model_key, request.headers.get('apim-subscription-id', 'Official'))

    def list_models(self, request):
        return self.get('/multivariate/models', request.headers.get('apim-subscription-id', 'Official'))

    def delete_model(self, request, model_key):
        return self.delete('/multivariate/models/' + model_key, request.headers.get('apim-subscription-id', 'Official'))

    def get_result(self, request, result_id):
        return self.get('/multivariate/result/' + result_id, request.headers.get('apim-subscription-id', 'Official'))
