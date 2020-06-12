from common.util.constant import DAY_IN_SECONDS, HOUR_IN_SECONDS, MINT_IN_SECONDS
from common.util.gran import Gran

MIN_HORIZON = 14
MAX_HORIZON = 120

MAX_LAG_LENGTH = 120

def get_max_horizon(gran, custom_in_seconds, future_target_size):
    if gran == Gran.Yearly:
        horizon = MIN_HORIZON
    elif gran == Gran.Monthly:
        horizon = MIN_HORIZON
    elif gran == Gran.Weekly:
        horizon = MIN_HORIZON
    elif gran == Gran.Daily:
        horizon = MIN_HORIZON
    elif gran == Gran.Hourly:
        horizon = DAY_IN_SECONDS * 4 / HOUR_IN_SECONDS
    elif gran == Gran.Minutely:
        horizon = HOUR_IN_SECONDS * 4 / MINT_IN_SECONDS
    elif gran == Gran.Secondly:
        horizon = MINT_IN_SECONDS
    else:
        horizon = HOUR_IN_SECONDS * 4 / custom_in_seconds

    horizon = max(horizon, future_target_size * 2)

    return int(min(horizon, MAX_HORIZON))


def get_max_lag(gran, window):
    if gran == Gran.Yearly:
        lag = window
    elif gran == Gran.Monthly:
        lag = window
    elif gran == Gran.Weekly:
        lag = window
    elif gran == Gran.Daily:
        lag = window
    elif gran == Gran.Hourly:
        lag = window
    elif gran == Gran.Minutely:
        lag = 7
    elif gran == Gran.Secondly:
        lag = 7
    else:
        lag = 7

    return int(min(lag, MAX_LAG_LENGTH))
