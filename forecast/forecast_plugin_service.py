import json
import shutil
import os
from telemetry import log

from common.plugin_service import PluginService
from common.util.constant import STATUS_SUCCESS, STATUS_FAIL
from common.util.timeutil import get_time_offset, str_to_dt, dt_to_str
from common.util.csv import save_to_csv
from common.util.metric import MetricSender
from common.util.fill_type import Fill
from common.util.gran import Gran

from forecast.model.inference import inference, load_inference_model, load_inference_input_data
from forecast.model.training import train

class ForecastPluginService(PluginService):

    def __init__(self):
        super().__init__()

    # Verify if the data could be used for this application
    # Parameters: 
    #   series_sets: a array of series set
    #   parameters: parameters of this application.
    # Return:
    #   result:  STATUS_FAIL / STATUS_SUCCESS
    #   message: a description of the result
    def do_verify(self, subscription, parameters):
        # ------TO BE REPLACED: Other application just replace below part-------
        # For forecast, check the factors and target has same granularity, and each factor could only contain one series
        meta = self.tsanaclient.get_metric_meta(parameters['apiKey'], parameters['instance']['params']['target']['metricId'])
        if meta is None: 
            return STATUS_FAIL, 'Target is not found. '
        target_gran = meta['granularityName']
        # Only for custom, the granularity amount is meaningful which is the number of seconds
        target_gran_amount = meta['granularityAmount']

        for data in parameters['seriesSets']: 
            if target_gran != data['metricMeta']['granularityName'] or (target_gran == 'Custom' and target_gran_amount != data['metricMeta']['granularityAmount']):
                return STATUS_FAIL, 'Granularity must be identical between target and factors. '

        # Check series count, and each factor should only contain 1 series
        seriesCount = 0
        for data in parameters['seriesSets']: 
            dim = {}
            for dimkey in data['dimensionFilter']: 
                dim[dimkey] = [data['dimensionFilter'][dimkey]]
            
            dt = dt_to_str(str_to_dt(meta['dataStartFrom']))
            para = dict(metricId=data['metricId'], dimensions=dim, count=2, startTime=dt)     # Let's said 100 is your limitation
            ret = self.tsanaclient.post(parameters['apiKey'], '/metrics/' + data['metricId'] + '/rank-series', data=para)
            if ret is None or 'value' not in ret:
                return STATUS_FAIL, 'Read series rank filed. '
            seriesCount += len(ret['value'])
            if seriesCount > self.config.series_limit:
                return STATUS_FAIL, 'Cannot accept ambiguous factors or too many series in the group, limit is ' + str(self.config.series_limit) + '.'

        return STATUS_SUCCESS, ''
            
    def do_train(self, subscription, model_id, model_dir, parameters):
        inference_window = parameters['instance']['params']['windowSize']
        meta = self.tsanaclient.get_metric_meta(parameters['apiKey'], parameters['instance']['params']['target']['metricId'])
        if meta is None:
            raise Exception('Metric is not found.')

        end_time = str_to_dt(parameters['endTime'])
        data_end_time = get_time_offset(end_time, (meta['granularityName'], meta['granularityAmount']),
                                + 1)

        start_time = get_time_offset(data_end_time, (meta['granularityName'], meta['granularityAmount']),
                                    - inference_window)

        if 'max_train_history_steps' in parameters['instance']['params']:
            backwindow = parameters['instance']['params']['max_train_history_steps']
        else:
            backwindow = self.config.lstm['train_history_step']
        start_time_train = get_time_offset(data_end_time, (meta['granularityName'], meta['granularityAmount']),
                                        - inference_window - backwindow)

        factor_def = parameters['seriesSets']
        factors_data = self.tsanaclient.get_timeseries(parameters['apiKey'], factor_def, start_time_train, data_end_time)
        
        target_def = [parameters['instance']['params']['target']]
        offset = 0
        if 'target_offset' in parameters['instance']['params']:
            offset = int(parameters['instance']['params']['target_offset'])
        target_data = self.tsanaclient.get_timeseries(parameters['apiKey'], target_def, start_time_train, data_end_time, offset, meta['granularityName'],
                                    meta['granularityAmount'])
        
        if factors_data is None or target_data is None:
            raise Exception('Error to get series.')

        inference_window, best_model = train(factor_series=factors_data,
                                            target_series=target_data[0],
                                            model_dir=model_dir,  # the model used to inference
                                            window=inference_window,
                                            timestamp=end_time,
                                            future_target_size=parameters['instance']['params']['step'],
                                            gran=Gran[meta['granularityName']],
                                            custom_in_seconds=meta['granularityAmount'],
                                            max_cores=2,
                                            metric_sender=MetricSender(self.config, subscription, model_id),
                                            epoc=parameters['instance']['params']['epoc'] if 'epoc' in
                                                                                                        parameters[
                                                                                                            'instance'][
                                                                                                            'params'] else self.config.lstm['epoc'],
                                            batch_size=parameters['instance']['params']['batch_size'] if 'batch_size' in
                                                                                                        parameters[
                                                                                                            'instance'][
                                                                                                            'params'] else self.config.lstm['batch_size'],
                                            steps_per_epoc=parameters['instance']['params']['steps_per_epoc'] if 'steps_per_epoc' in
                                                                                                        parameters[
                                                                                                            'instance'][
                                                                                                            'params'] else self.config.lstm['steps_per_epoc'],              
                                            validation_freq=parameters['instance']['params']['validation_freq'] if 'validation_freq' in
                                                                                                        parameters[
                                                                                                            'instance'][
                                                                                                            'params'] else self.config.lstm['validation_freq'],       
                                            validation_ratio=parameters['instance']['params']['validation_ratio'] if 'validation_ratio' in
                                                                                                        parameters[
                                                                                                            'instance'][
                                                                                                            'params'] else self.config.lstm['validation_ratio'],              
                                            num_hidden=parameters['instance']['params']['num_hidden'] if 'num_hidden' in
                                                                                                        parameters[
                                                                                                            'instance'][
                                                                                                            'params'] else self.config.lstm['num_hidden'],        
                                            fill_type=Fill[parameters['instance']['params']['fill']] if 'fill' in
                                                                                                        parameters[
                                                                                                            'instance'][
                                                                                                            'params'] else Fill.Previous,
                                            fill_value=parameters['instance']['params']['fillValue'] if 'fillValue' in
                                                                                                        parameters[
                                                                                                            'instance'][
                                                                                                            'params'] else 0
                                            )
        # Need to call callback
        return STATUS_SUCCESS, ''

    def do_inference(self, subscription, model_id, model_dir, parameters):
        log.info("Start to inference %s", model_dir)
        inference_window = parameters['instance']['params']['windowSize']
        meta = self.tsanaclient.get_metric_meta(parameters['apiKey'], parameters['instance']['params']['target']['metricId'])
        if meta is None: 
            return STATUS_FAIL, 'Metric is not found. '
        end_time = str_to_dt(parameters['endTime'])
        if 'startTime' in parameters: 
            start_time  = str_to_dt(parameters['startTime'])
        else: 
            start_time = end_time
        cur_time = start_time

        data_end_time = get_time_offset(end_time, (meta['granularityName'], meta['granularityAmount']),
                                                    + 1)

        data_start_time = get_time_offset(start_time, (meta['granularityName'], meta['granularityAmount']),
                                                    - inference_window * 2)


        factor_def = parameters['seriesSets']
        factors_data = self.tsanaclient.get_timeseries(parameters['apiKey'], factor_def, data_start_time, data_end_time)

        target_def = [parameters['instance']['params']['target']]
        target_data = self.tsanaclient.get_timeseries(parameters['apiKey'], target_def, data_start_time, data_end_time)

        model, window = load_inference_model(model_dir=model_dir, target_size=parameters['instance']['params']['step'],
                            window=inference_window, 
                            metric_sender=MetricSender(self.config, subscription, model_id), 
                            epoc=parameters['instance']['params']['epoc'] if 'epoc' in
                                                                                                parameters[
                                                                                                    'instance'][
                                                                                                    'params'] else self.config.lstm['epoc'],
                            validation_freq=parameters['instance']['params']['validation_freq'] if 'validation_freq' in
                                                                                                parameters[
                                                                                                    'instance'][
                                                                                                    'params'] else self.config.lstm['validation_freq'],     
                            validation_ratio=parameters['instance']['params']['validation_ratio'] if 'validation_ratio' in
                                                                                                parameters[
                                                                                                    'instance'][
                                                                                                    'params'] else self.config.lstm['validation_ratio'])

        input_data = load_inference_input_data(target_series=target_data[0],factor_series=factors_data, 
                                            model=model, gran=Gran[meta['granularityName']], 
                                            custom_in_seconds=meta['granularityAmount'], 
                                            fill_type=Fill[parameters['instance']['params']['fill']] if 'fill' in
                                                                                                        parameters[
                                                                                                            'instance'][
                                                                                                            'params'] else Fill.Previous,
                                            fill_value=parameters['instance']['params']['fillValue'] if 'fillValue' in
                                                                                                        parameters[
                                                                                                            'instance'][
                                                                                                            'params'] else 0)
        while cur_time <= end_time: 
            try: 
                result = inference(input_data=input_data, window=window, timestamp=cur_time, 
                                            target_size=parameters['instance']['params']['step'], model=model)
                    
                if len(result) > 0: 
                    # offset back
                    if 'target_offset' in parameters['instance']['params']:
                        offset = int(parameters['instance']['params']['target_offset'])
                        for idx in range(len(result)): 
                            result[idx]['timestamp'] = dt_to_str(get_time_offset(cur_time, (meta['granularityName'], meta['granularityAmount']),
                                                                                        - offset + idx))
                            # print(result[idx]['timestamp'])
                    self.tsanaclient.save_inference_result(parameters, result)
                else:
                    log.error("No result for this inference %s, key %s" % (dt_to_str(cur_time), model_dir))
                # process = psutil.Process(os.getpid())
                # print(process.memory_info().rss)
            except Exception as e: 
                log.error("-------Inference exception-------")
            
            cur_time = get_time_offset(cur_time, (meta['granularityName'], meta['granularityAmount']),
                                                            + 1)
        return STATUS_SUCCESS, ''

    def get_inference_time_range(self, parameters):
        raise Exception('Not implemented!')