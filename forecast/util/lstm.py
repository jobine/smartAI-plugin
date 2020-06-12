import os
import pickle

from tensorflow import data
from tensorflow_core.python.keras.layers.core import Dense
from tensorflow_core.python.keras.layers.recurrent import LSTM
from tensorflow_core.python.keras.models import Sequential
import numpy as np

import pandas as pd

from forecast.util.model_type import ModelType
from forecast.util.multivariate import MultivariateData
from forecast.util.univariate_forecast_item import UnivariateForecastItem

from common.util.timeutil import get_time_offset, str_to_dt, dt_to_str
from common.util.timeutil import convert_freq
from common.util.metric import MetricCollector
from common.util.constant import TIMESTAMP, VALUE

class LSTMModel:
    def __init__(self, num_hidden, window, end_time, future_target_size, validation_ratio, validation_freq,
                 effective_factor, mean_absolute_percentage_error=None, describe=None, epoc=10, metric_sender=None):
        self.__num_hidden = num_hidden
        self.__future_target = future_target_size
        self.__window = window
        self.__epochs = epoc
        self.__mean_absolute_percentage_error = mean_absolute_percentage_error
        self.__end_time = end_time
        self.__effective_factor = effective_factor
        self.__model = Sequential([
            LSTM(num_hidden, input_shape=np.zeros((window, len(effective_factor))).shape),
            Dense(self.__future_target)
        ])
        self.__model.compile(optimizer='adam', loss='mean_squared_error')
        self.__validation_ratio = validation_ratio
        self.__validation_freq = validation_freq
        self.__fit_model = ModelType.LSTM
        self.__describe = describe
        self.__metric_collector = MetricCollector(epochs=self.__epochs, metric_sender=metric_sender)


    def train(self, input_data: MultivariateData, batch_size, steps_per_epoc):
        input_factors = input_data.generate_outer_join_factors()
        input_factors = MultivariateData.generate_filled_missing_frame(input_factors,
                                                                       input_data.get_gran(),
                                                                       input_data.get_custom_in_seconds(),
                                                                       fill_type=input_data.fill_type,
                                                                       fill_value=input_data.fill_value
                                                                       )
                                                                
        merged_input = MultivariateData.generate_inner_join_frame([input_data.get_target(),
                                                                   input_factors])
        input_target = merged_input[[TIMESTAMP, VALUE]]
        input_factors = merged_input.drop([TIMESTAMP, VALUE], axis=1)
        input_factors = input_factors.reindex(columns=self.__effective_factor)
        self.__describe = merged_input.describe().T
        train, label = input_data.get_normalized_batch(self.__window, self.__future_target, label=input_target,
                                                       factors=input_factors)
        batch_size = int(min(batch_size, max(1, len(train) / steps_per_epoc)))
        train_multi = data.Dataset.from_tensor_slices((train[: -int(len(train) *
                                                                    self.__validation_ratio)],
                                                       label[: -int(len(label) *
                                                                    self.__validation_ratio)]))

        train_multi = train_multi.cache().shuffle(len(train) * 100).batch(batch_size).repeat()

        val_multi = data.Dataset.from_tensor_slices((train[-int(len(train) *
                                                                self.__validation_ratio):],
                                                     label[-int(len(label) *
                                                                self.__validation_ratio):]))
        val_multi = val_multi.cache().batch(batch_size).repeat()

        self.__model.fit(train_multi, epochs=self.__epochs, shuffle=False,
                         validation_data=val_multi,
                         validation_freq=self.__validation_freq,
                         steps_per_epoch=len(train) * (1 - self.__validation_ratio) / batch_size,
                         validation_steps=len(train) * self.__validation_ratio / batch_size,
                         callbacks=[self.__metric_collector]
                         )
        validation_result = self.__model.predict(train[-int(len(train) * self.__validation_ratio):])
        validation_labels = label[-int(len(label) * self.__validation_ratio):]
        mean_average_percentage_error = np.abs(validation_result - validation_labels) / np.abs(validation_labels)
        mean_average_percentage_error[np.isinf(mean_average_percentage_error)] = np.nan
        mean_average_percentage_error = np.nanmean(mean_average_percentage_error, axis=0)
        self.__mean_absolute_percentage_error = mean_average_percentage_error

    def get_mean_absolute_percentage_error(self):
        return list(self.__mean_absolute_percentage_error)

    def get_effective_factor(self):
        return self.__effective_factor

    def save_model(self, model_dir):
        with open(os.path.join(model_dir, 'LSTM-Meta.pkl'), "wb") as f:
            meta = {
                'mean_absolute_percentage_error': self.__mean_absolute_percentage_error,
                'end_time': self.__end_time,
                'future_target': self.__future_target,
                'window': self.__window,
                'effective_factor': self.__effective_factor,
                'num_hidden': self.__num_hidden,
                'describe': self.__describe
            }
            pickle.dump(meta, f)
        self.__model.save_weights(os.path.join(model_dir, self.__fit_model.name))

    def get_model_type(self):
        return self.__fit_model

    @staticmethod
    def load_model_meta(model_dir):
        with open(os.path.join(model_dir, 'LSTM-Meta.pkl'), "rb") as f:
            meta = pickle.load(f)
        return {
            'mean_absolute_percentage_error': meta['mean_absolute_percentage_error'],
            'end_time': meta['end_time'],
            'window': meta['window'],
            'effective_factor': meta['effective_factor'],
            'future_target': meta['future_target'],
            'num_hidden': meta['num_hidden'],
            'describe': meta['describe']
        }

    def inference(self, input_data: MultivariateData, window, timestamp, **kwargs):
        input_factors = input_data.generate_outer_join_factors()
        if timestamp is None: 
            ts = input_factors[TIMESTAMP].max() 
        else:
            ts = pd.to_datetime(timestamp)
            ts = ts.tz_localize(None)
        
        input_factors = MultivariateData.gen_filled_missing_by_period(input_factors, input_data.get_gran(),
                                                                      input_data.get_custom_in_seconds(),
                                                                      end_time=ts,
                                                                      periods=window,
                                                                      fill_type=input_data.fill_type,
                                                                      fill_value=input_data.fill_value
                                                                      )
        input_factors = input_factors[self.__effective_factor]
        input_factors = input_factors.reindex(columns=self.__effective_factor)
        

        input_factors = input_factors.tail(window)
        # print(input_factors)
        for column in self.__effective_factor:
            min_value = self.__describe.loc[column]['min']
            max_value = self.__describe.loc[column]['max']
            if max_value == min_value:
                input_factors[column] = 0
            else:
                input_factors[column] = (input_factors[column] - min_value) / (max_value - min_value)
        input_factors = input_factors.values
        input_factors[(input_factors < 0) | (input_factors > 1)] = 0
        predicted = self.__model.predict(np.array([input_factors]))
        predicted = predicted.reshape(self.__future_target)
        predicted = predicted * (self.__describe.loc[VALUE]['max'] -
                                 self.__describe.loc[VALUE]['min']) + self.__describe.loc[VALUE]['min']
        target_timestamps = pd.date_range(start=timestamp, periods=self.__future_target,
                                          freq=convert_freq(input_data.get_gran(),
                                                            input_data.get_custom_in_seconds()))
        lower_boundary = [predicted[i] - np.abs(predicted[i]) * self.__mean_absolute_percentage_error[i]
                          for i in range(0, len(predicted))]
        upper_boundary = [predicted[i] + np.abs(predicted[i]) * self.__mean_absolute_percentage_error[i]
                          for i in range(0, len(predicted))]
        return [UnivariateForecastItem(predicted[i], lower_boundary[i], upper_boundary[i],
                                       (1 - self.__mean_absolute_percentage_error[i]),
                                       timestamp=target_timestamps[i]).to_dict()
                for i in range(0, len(predicted))]

    def load_model(self, model_dir):
        self.__model.load_weights(os.path.join(model_dir, self.__fit_model.name))

    def get_end_time(self):
        return self.__end_time
