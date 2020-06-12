from functools import reduce
import json
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import pandas as pd

from common.util.fill_type import Fill
from common.util.series import Series
from common.util.timeutil import convert_freq
from common.util.constant import VALUE, TIMESTAMP

def fill_missing(input_series, fill_type: Fill, fill_value):
    if fill_type == Fill.NotFill:
        return input_series
    if fill_type == Fill.Previous:
        return input_series.fillna(method='ffill', limit=len(input_series)).fillna(method='bfill',
                                                                                   limit=len(input_series))
    if fill_type == Fill.Subsequent:
        return input_series.fillna(method='bfill', limit=len(input_series)).fillna(method='ffill',
                                                                                   limit=len(input_series))
    if fill_type == Fill.Linear:
        return input_series.interpolate(method='linear', limit_direction='both', axis=0, limit=len(input_series))
    if fill_type == Fill.Pad:
        return input_series.fillna(fill_value)

    return input_series.fillna(0)


class MultivariateData:
    def __init__(self, target: Series, factors, gran, custom_in_seconds, effective_factors=None, fill_type=Fill.Linear,
                 fill_value=0):
        self.__target = pd.DataFrame(target.value)
        self.__target = self.__target[[TIMESTAMP, VALUE]]
        self.__target[TIMESTAMP] = pd.to_datetime(self.__target[TIMESTAMP])
        self.__target[TIMESTAMP] = self.__target[TIMESTAMP].dt.tz_localize(None)
        self.__factors = {}
        for factor in factors:
            if effective_factors is not None and factor.name not in effective_factors:
                continue
            self.__factors[factor.name] = factor.values

        self.__effective_factors = [x for x in sorted(self.__factors.keys())] \
            if effective_factors is None else effective_factors
        self.__gran = gran
        self.__custom_in_seconds = custom_in_seconds
        self.__fill_type = fill_type
        self.__fill_value = fill_value

    @property
    def fill_type(self):
        return self.__fill_type

    @property
    def fill_value(self):
        return self.__fill_value

    def get_target(self):
        return self.__target

    def get_gran(self):
        return self.__gran

    def get_custom_in_seconds(self):
        return self.__custom_in_seconds

    def get_effective_factor(self):
        return self.__effective_factors

    def generate_inner_join_factors(self):
        return MultivariateData.generate_inner_join_frame(self.__factors.values())

    def generate_outer_join_factors(self):
        return MultivariateData.generate_outer_join_frame(self.__factors.values(), self.__fill_type, self.__fill_value)

    @staticmethod
    def generate_filled_missing_frame(input_frame, gran, custom_in_seconds, fill_type: Fill, fill_value):
        if fill_type == Fill.NotFill:
            return input_frame
        full_data_range = pd.date_range(start=input_frame[TIMESTAMP].min(), end=input_frame[TIMESTAMP].max(),
                                        freq=convert_freq(gran, custom_in_seconds))
        full_data_range = pd.DataFrame(full_data_range, columns=[TIMESTAMP])
        input_frame = pd.merge(full_data_range, input_frame, how='left', on=TIMESTAMP)
        return fill_missing(input_frame, fill_type=fill_type, fill_value=fill_value)

    @staticmethod
    def gen_filled_missing_by_period(input_frame, gran, custom_in_seconds, end_time, periods, fill_type: Fill,
                                     fill_value):
        if fill_type == Fill.NotFill:
            return input_frame
        full_data_range = pd.date_range(end=end_time, freq=convert_freq(gran, custom_in_seconds), periods=periods)
        full_data_range = pd.DataFrame(full_data_range, columns=[TIMESTAMP])
        input_frame = pd.merge(full_data_range, input_frame, how='left', on=TIMESTAMP)
        return fill_missing(input_frame, fill_type, fill_value)

    @staticmethod
    def generate_inner_join_frame(input_frames):
        return reduce(lambda left, right: pd.merge(left, right, on=TIMESTAMP, how='inner'), input_frames)

    @staticmethod
    def generate_outer_join_frame(input_frames, fill_type: Fill, fill_value):
        if fill_type == Fill.NotFill:
            return MultivariateData.generate_inner_join_frame(input_frames)
        merged = reduce(lambda left, right: pd.merge(left, right, on=TIMESTAMP, how='outer'), input_frames)
        return fill_missing(merged, fill_type, fill_value)

    @staticmethod
    def get_normalized_batch(sliding_window, future_target_step, label, factors):
        scale = MinMaxScaler(feature_range=(0, 1))
        label = np.array(label[VALUE])
        label = scale.fit_transform(label.reshape(-1, 1))
        label = label.reshape(len(label))
        train = scale.fit_transform(factors)
        np.nan_to_num(train, copy=False)
        batch_train = np.array([train[i: i + sliding_window]
                                for i in range(0, len(train) - (sliding_window + future_target_step) + 1)])
        batch_labels = np.array([label[i:i + future_target_step] for i in range(sliding_window,
                                                                                len(label) - future_target_step + 1)])
        return batch_train, batch_labels

    @staticmethod
    def get_batch(sliding_window, future_target_step, label, factors):
        label = np.array(label[VALUE])
        label = label.reshape(len(label))
        train = factors.values
        batch_train = np.array([train[i: i + sliding_window]
                                for i in range(0, len(train) - (sliding_window + future_target_step) + 1)])
        batch_labels = np.array([label[i:i + future_target_step] for i in range(sliding_window,
                                                                                len(label) - future_target_step + 1)])
        return batch_train, batch_labels
