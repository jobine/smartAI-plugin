from common.util.fill_type import Fill
from forecast.util.forecast_factor import ForecastFactor
from forecast.util.lstm import LSTMModel
from forecast.util.model_type import ModelType
from forecast.util.multivariate import MultivariateData
from forecast.util.univariate import UnivariateData
from forecast.util.util import get_max_horizon, get_max_lag

def train(target_series, factor_series, window, model_dir, timestamp, future_target_size,
          gran, custom_in_seconds, max_cores, metric_sender, epoc, batch_size, steps_per_epoc, validation_freq, validation_ratio, num_hidden, fill_type: Fill, fill_value):
    train_data = MultivariateData(target_series, [ForecastFactor(x) for x in factor_series],
                                  gran=gran, custom_in_seconds=custom_in_seconds, fill_type=fill_type,
                                  fill_value=fill_value)
    if len(train_data.get_effective_factor()) == 0:
        models = [ModelType.AutoMLNoLags]
    else:
        models = [ModelType.LSTM]

    trained_models = {}
    for model_type in models:
        model = None
        if model_type == ModelType.LSTM:
            model = LSTMModel(num_hidden=num_hidden,
                              window=window, end_time=timestamp, validation_ratio=validation_ratio,
                              validation_freq=validation_freq, future_target_size=future_target_size,
                              effective_factor=train_data.get_effective_factor(), metric_sender=metric_sender, epoc=epoc)
            model.train(input_data=train_data, batch_size=batch_size,
                        steps_per_epoc=steps_per_epoc)
        model.save_model(model_dir)
        trained_models[model_type] = model

    best_model_type = None
    best_mean_absolute_percentage_error = None
    for model_type in trained_models.keys():
        mean_absolute_percentage_error = trained_models[model_type].get_mean_absolute_percentage_error()
        if best_model_type is None:
            best_model_type = model_type
            best_mean_absolute_percentage_error = mean_absolute_percentage_error
        elif best_mean_absolute_percentage_error > mean_absolute_percentage_error:
            best_mean_absolute_percentage_error = mean_absolute_percentage_error
            best_model_type = model_type

    best_model = trained_models[best_model_type]

    return window, best_model
