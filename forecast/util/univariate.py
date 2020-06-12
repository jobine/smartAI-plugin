from sklearn.preprocessing import MinMaxScaler
import pandas as pd


class UnivariateData:
    def __init__(self, series):
        self.__series = series
        self.__size = len(series)

    def generate_batch(self, batch_size, history_size, target_size):
        total_size = history_size + target_size
        segments = [self.__series[i: i + total_size] for i in range(0, self.__size - total_size)]
        scaler = MinMaxScaler(feature_range=(-1, 1))
        scaled_segments = scaler.fit_transform(segments)
        batch_series = [x[:history_size] for x in scaled_segments]
        batch_labels = [x[history_size:] for x in scaled_segments]
        batch_series = [batch_series[i: i + batch_size] for i in range(0, len(batch_series) - batch_size)]
        batch_labels = [batch_labels[i: i + batch_size] for i in range(0, len(batch_labels) - batch_size)]

        return batch_series, batch_labels

    def get_series(self):
        return self.__series

    def generate_frame(self):
        return pd.DataFrame(self.__series)
