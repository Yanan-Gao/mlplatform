from enum import Enum

TRAIN = "train"
VAL = "validation"
TEST = "test"

class ModelHeads(Enum):
    CPD_FLOOR_WIN = "cpd_floor_win"
    CPD_FLOOR = "cpd_floor"
    CPD = "cpd"
    REG = "reg"

    @staticmethod
    def from_str(s):
        if s.lower() == "cpd":
            return ModelHeads.CPD
        elif s.lower() == "cpd_floor":
            return ModelHeads.CPD_FLOOR
        elif s.lower() == "cpd_floor_win":
            return ModelHeads.CPD_FLOOR_WIN
        elif s.lower() == "reg":
            return ModelHeads.REG
        else:
            raise NotImplementedError


class CpdType(Enum):
    LOGNORM = "lognorm"
    MIXTURE = "mixture"

    @staticmethod
    def from_str(s):
        if s.lower() == "lognorm":
            return CpdType.LOGNORM
        elif s.lower() == "mixture":
            return CpdType.MIXTURE
        else:
            raise NotImplementedError
