import enum

class ModelState(enum.Enum):
    TRAINING = 1
    READY = 2
    DELETED = 3
    FAILED = 4

class InferenceState(enum.Enum):
    PENDING = 1
    SUCCESS = 2
    FAIL = 3

STATUS_SUCCESS = 'Success'
STATUS_FAIL = 'Fail'

TIMESTAMP = 'timestamp'
VALUE = 'value'

LAG = 'lag'
FORECAST = 'forecast'
UPPER = 'upper'
LOWER = 'lower'

DAY_IN_SECONDS = 86400
HOUR_IN_SECONDS = 3600
MINT_IN_SECONDS = 60