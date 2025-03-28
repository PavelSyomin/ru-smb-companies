import enum


class SourceDatasets(enum.Enum):
    sme = "sme"
    revexp = "revexp"
    empl = "empl"


class StageNames(enum.Enum):
    download = "download"
    extract = "extract"
    aggregate = "aggregate"
    geocode = "geocode"
    panelize = "panelize"


class Storages(enum.Enum):
    local = "local"
    ydisk = "ydisk"
