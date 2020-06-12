import enum


# Using enum class create enumerations
class ModelType(enum.Enum):
    AutoMLNoLags = 1
    AutoMLLags = 2
    LSTM = 3
