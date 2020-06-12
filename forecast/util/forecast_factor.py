import pandas as pd
from common.util.constant import TIMESTAMP, VALUE
from common.util.series import Series


class ForecastFactor:
    def __init__(self, series: Series):
        self.name = series.series_id
        self.metrics = series.metric_id
        self.tags = series.dim
        self.values = pd.DataFrame(series.value)
        self.values = self.values[[TIMESTAMP, VALUE]]
        self.values[TIMESTAMP] = pd.to_datetime(self.values[TIMESTAMP])
        self.values[TIMESTAMP] = self.values[TIMESTAMP].dt.tz_localize(None)
        self.values = self.values.rename(columns={VALUE:  self.name})
