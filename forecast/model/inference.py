
from common.util.fill_type import Fill
# from Algorithms.forecast.models.automl import AutoML
from forecast.util.forecast_factor import ForecastFactor
from forecast.util.lstm import LSTMModel
from forecast.util.model_type import ModelType
from forecast.util.multivariate import MultivariateData
# from Algorithms.forecast.models.prophet import ProphetModel

def load_inference_model(model_dir, target_size,
              window, metric_sender, epoc, validation_freq, validation_ratio):
    meta = LSTMModel.load_model_meta(model_dir)
    window = meta['window']
    model = LSTMModel(num_hidden=meta['num_hidden'], window=meta['window'],
                        end_time=meta['end_time'], future_target_size=meta['future_target'],
                        effective_factor=meta['effective_factor'],
                        mean_absolute_percentage_error=meta['mean_absolute_percentage_error'],
                        describe=meta['describe'],
                        validation_freq=validation_freq,
                        validation_ratio=validation_ratio,
                        epoc=epoc,
                        metric_sender=metric_sender
                        )
    model.load_model(model_dir)
    
    return model, window

def load_inference_input_data(target_series, factor_series, model, gran, custom_in_seconds, fill_type: Fill, fill_value):
    return MultivariateData(target_series, [ForecastFactor(x) for x in factor_series],
                                  effective_factors=model.get_effective_factor(), gran=gran,
                                  custom_in_seconds=custom_in_seconds, fill_type=fill_type, fill_value=fill_value)

def inference(input_data: MultivariateData, window, timestamp, target_size, model):
    return model.inference(input_data=input_data, window=window, timestamp=timestamp, target_size=target_size)
