from common.plugin_service import PluginService
from common.util.constant import STATUS_SUCCESS, STATUS_FAIL
from common.util.timeutil import get_time_offset, str_to_dt, dt_to_str
from telemetry import log
import copy


class DemoService(PluginService):
    def __init__(self):
        super().__init__()

    def do_verify(self, subscription, parameters):
        # Check series set permission
        for data in parameters['seriesSets']:
            meta = self.tsanaclient.get_metric_meta(parameters['apiKey'], data['metricId'])

            if meta is None:
                return STATUS_FAIL, 'You have no permission to read Metric {}'.format(data['metricId'])

            return STATUS_SUCCESS

    def do_inference(self, subscription, model_id, model_dir, parameters):
        log.info('Start to inference {}'.format('Demo'))
        try:
            amplifier = parameters['instance']['params']['amplifier']
            end_time = str_to_dt(parameters['endTime'])
            if 'startTime' in parameters:
                start_time = str_to_dt(parameters['startTime'])
            else:
                start_time = end_time

            series = self.tsanaclient.get_timeseries(parameters['apiKey'], parameters['seriesSets'], start_time, end_time)

            copied = copy.deepcopy(series)

            for data in copied:
                data.value = data.value * amplifier

            self.tsanaclient.save_inference_result(parameters, copied)

            return STATUS_SUCCESS, ''
        except Exception as e:
            log.error('Exception thrown by inference: ' + repr(e))
            return STATUS_FAIL, 'Exception thrown by inference: ' + repr(e)

