from common.util.constant import TIMESTAMP

FORECAST_VALUE = 'forecastValue'
CONFIDENCE = 'confidence'
UPPER_BOUNDARY = 'upperBoundary'
LOWER_BOUNDARY = 'lowerBoundary'

class UnivariateForecastItem:
    def __init__(self, forecast_value, lower_boundary, upper_boundary, confidence, timestamp):
        self.forecast_value = float(forecast_value)
        self.confidence = float(confidence)
        self.upper_boundary = float(upper_boundary)
        self.lower_boundary = float(lower_boundary)
        self.timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')

    def to_dict(self):
        return {FORECAST_VALUE: self.forecast_value, CONFIDENCE: self.confidence,
                UPPER_BOUNDARY: self.upper_boundary, LOWER_BOUNDARY: self.lower_boundary, TIMESTAMP: self.timestamp}
