import enum
import json
import pathlib
from typing import List, Optional
from typing_extensions import Annotated

import typer

from stages.aggregate import Aggregator
from stages.download import Downloader
from stages.extract import Extractor
from stages.georeference import Georeferencer
from stages.panelize import Panelizer


APP_NAME = "ru_smb_companies"

app = typer.Typer(
    help="Create dataset of Russian SMB companies (and individuals) based on Federal Tax Service's open data",
    rich_markup_mode="markdown"
)

app_dir = typer.get_app_dir(APP_NAME)
app_config_path = pathlib.Path(app_dir) / "config.json"
app_config_path.parent.mkdir(parents=True, exist_ok=True)
if app_config_path.exists():
    try:
        with open(app_config_path) as f:
            app_config = json.load(f)
    except:
        app_config = {}
        print("Failed to load config")
else:
    app_config = dict(storage="local", token=None, num_workers=1, chunksize=16)


def get_default_path(stage_name: str, mode: str) -> str:
    return str(pathlib.Path("ru-smb-data") / stage_name / mode)


class StageNames(enum.Enum):
    download = "download"
    extract = "extract"
    aggregate = "aggregate"
    georeference = "georeference"
    panelize = "panelize"


class Storages(enum.Enum):
    local = "local"
    ydisk = "ydisk"


class SourceDatasets(enum.Enum):
    smb = "smb"
    revexp = "revexp"
    empl = "empl"


@app.command(rich_help_panel="Stages")
def download(
    source_dataset: Annotated[
        Optional[SourceDatasets],
        typer.Option(
            help="Label of the source dataset in FTS open data: **smb** is small&medium-sized businesses registry, **revexp** is data on revenue and expenditure of organizations, **empl** is data on conut of employees of organizations. If option is not specified, than all three datasets are downloaded",
            show_default="all three source datasets"
        )
    ] = None,
    download_dir: Annotated[
        Optional[str],
        typer.Option(
            help="Path to the directory to store downloaded files. If not specified, the default path *ru-smb-data/download/{source_dataset}* is used. If source_dataset is not specified, the default path is always used, even if *download_dir* is set"
        )
    ] = None
):
    """
    Download source dataset(s) from FTS open data server (stage 1)
    """
    storage = app_config.get("storage")
    token = app_config.get("token")
    if storage in ("ydisk", ) and token is None:
        raise RuntimeError("Token is required to use ydisk storage")

    d = Downloader(token)

    if source_dataset is None:
        for source_dataset in SourceDatasets:
            download_dir = get_default_path(StageNames.download.value, source_dataset.value)
            d(storage, source_dataset.value, download_dir)
    else:
        download_dir = download_dir or get_default_path(STAGE_NAME, source_dataset)
        d(storage, source_dataset, download_dir)


@app.command(rich_help_panel="Stages")
def extract(
    in_dir: Annotated[
        Optional[str],
        typer.Option(
            help="Path to downloaded source files. Usually the same as *download_dir* on download stage. If not specified, default auto-generated path *ru-smb-data/download/{source_dataset}* is used. If *source_dataset* option (see below) is not specified, default auto-generated path is always used",
            show_default="auto-generated in form ru-smb-data/download/{source_dataset}"
        )
    ] = None,
    out_dir: Annotated[
        Optional[str],
        typer.Option(
            help="Path to save extracted CSV files. If not specified, auto-generated path *ru-smb-data/extract/{source_dataset}* is used. If *source_dataset* option (see below) is not specified, default auto-generated path is always used",
            show_default="auto-generated in form ru-smb-data/extract/{source_dataset}"
        )
    ] = None,
    source_dataset: Annotated[
        Optional[SourceDatasets],
        typer.Option(
            help="Label of the source dataset in FTS open data: **smb** is small&medium-sized businesses registry, **revexp** is data on revenue and expenditure of organizations, **empl** is data on conut of employees of organizations. If option is not specified, than all three datasets are downloaded",
            show_default="all three source datasets"
        )
    ] = None,
    clear: Annotated[
        bool, typer.Option(help="Clear *out_dir* (see above) before processing")
    ] = False,
    ac: Annotated[
        Optional[List[str]],
        typer.Option(
            help="**A**ctivity **c**ode(s) to filter smb source dataset by. Can be either activity group code, e.g. *--ac A*, or exact digit code, e.g. *--ac 01.10*. Multiple codes or groups can be specified by multiple *ac* options, e.g. *--ac 01.10 --ac 69.20*. Top-level codes include child codes, i.e. *--ac 01.10* selects 01.10.01, 01.10.02, 01.10.10 (if any children are present). If not specified, filtering is disabled. If *source_dataset* (see above) is *revexp* or *empl*, filtering is not performed",
            show_default="no filtering by activity code(s)"
        )
    ] = None,
):
    """
    Extract data from downloaded source datasets (*zip* archives) to *csv* files,
    optionally filtering by activity code (stage 2)
    """
    num_workers = app_config.get("num_workers", 1)
    chunksize = app_config.get("chunksize", 16)
    storage = app_config.get("storage")
    token = app_config.get("token")

    if storage in ("ydisk",) and token is None:
        raise RuntimeError("Token is required to use ydisk storage")

    e = Extractor(storage, num_workers, chunksize, token)
    if source_dataset is None:
        for source_dataset in SourceDatasets:
            in_dir = get_default_path("download", source_dataset.value)
            out_dir = get_default_path(StageNames.extract.value, source_dataset.value)
            e(in_dir, out_dir, source_dataset.value, clear, ac)
    else:
        in_dir = in_dir or get_default_path("download", source_dataset.value)
        out_dir = out_dir or get_default_path(StageNames.extract.value, source_dataset.value)
        e(in_dir, out_dir, source_dataset.value, clear, ac)


@app.command(rich_help_panel="Stages")
def aggregate(
    in_dir: Annotated[
        Optional[str],
        typer.Option(
            help="Path to extracted CSV files. Usually the same as *out_dir* on extract stage. If not specified, default auto-generated path *ru-smb-data/extract/{source_dataset}* is used. If *source_dataset* option (see below) is not specified, default auto-generated path is always used",
            show_default="auto-generated in form ru-smb-data/extract/{source_dataset}"
        )
    ] = None,
    out_file: Annotated[
        Optional[str],
        typer.Option(
            help="Path to aggregated CSV files (including file name with extension). If not specified, default auto-generated path *ru-smb-data/aggregate/{source_dataset}/agg.csv* is used. If *source_dataset* option (see below) is not specified, default auto-generated path is always used",
            show_default="auto-generated in form ru-smb-data/aggregate/{source_dataset}/agg.csv"
        )
    ] = None,
    source_dataset: Annotated[
        Optional[str],
        typer.Option(
            help="Label of the source dataset in FTS open data: **smb** is small&medium-sized businesses registry, **revexp** is data on revenue and expenditure of organizations, **empl** is data on conut of employees of organizations. If option is not specified, than all three datasets are downloaded",
            show_default="all three source datasets"
        )
    ] = None,
    smb_data_file: Annotated[
        Optional[pathlib.Path],
        typer.Option(
            help="If *source_dataset* (see above) is *revexp* or *empl*, this option sets the path to **already processed smb file** that is used to filter aggregated values in revexp or empl file. Apparently, this file must exist. If *source_dataset* is *smb*, the option has no effect",
            show_default="auto-generated in form ru-smb-data/aggregate/smb/agg.csv",
            exists=True,
            file_okay=True,
            readable=True
        )
    ] = None
):
    """
    Aggregate extracted CSV files into a single CSV file removing duplicates (stage 3)
    """
    a = Aggregator()
    if source_dataset is None:
        for source_dataset in SourceDatasets:
            in_dir = get_default_path(StageNames.extract.value, source_dataset.value)
            out_file = str(pathlib.Path(get_default_path(StageNames.aggregate.value, source_dataset.value)) / "agg.csv")
            if source_dataset.value in ("revexp", "empl"):
                a(in_dir, out_file, source_dataset.value, str(smb_data_file))
            else:
                a(in_dir, out_file, source_dataset.value)
    else:
        in_dir = in_dir or get_default_path(StageNames.extract.value, source_dataset.value)
        out_file = out_file or str(pathlib.Path(get_default_path(StageNames.aggregate.value, source_dataset.value)) / "agg.csv")
        a(in_dir, out_file, source_dataset.value, str(smb_data_file))


@app.command(rich_help_panel="Stages")
def georeference(
    in_file: Annotated[
        Optional[pathlib.Path],
        typer.Option(
            help="Path to aggregated CSV files. Usually the same as *out_file* on aggregate stage with *--source-dataset smb*. If not specified, default auto-generated path *ru-smb-data/aggregate/smb/agg.csv* is used",
            show_default="auto-generated in form ru-smb-data/aggregate/smb/agg.csv",
            exists=True,
            file_okay=True,
            readable=True
        )
    ] = None,
    out_file: Annotated[
        Optional[str],
        typer.Option(
            help="Path to save georeferenced CSV files.If not specified, default auto-generated path *ru-smb-data/georeference/smb/georeferenced.csv* is used",
            show_default="auto-generated in form ru-smb-data/georeference/smb/georeferenced.csv"
        )
    ] = None
):
    """
    Georeference SMB aggregated data (stage 4)
    """
    g = Georeferencer()
    in_file = str(in_file) or str(pathlib.Path(get_default_path(StageNames.aggregate.value, SourceDatasets.smb.value)) / "agg.csv")
    out_file = out_file or str(pathlib.Path(get_default_path(StageNames.georeference.value, SourceDatasets.smb.value)) / "georeferenced.csv")
    d(in_file, out_file)


@app.command(rich_help_panel="Stages")
def panelize(
    smb_file: Annotated[
        Optional[pathlib.Path],
        typer.Option(
            help="Path to georeferenced CSV files. Usually the same as *out_file* on georeference stage. If not specified, default auto-generated path *ru-smb-data/georeference/smb/georeferenced.csv* is used",
            show_default="auto-generated in form ru-smb-data/georeference/smb/georeferenced.csv",
            exists=True,
            file_okay=True,
            readable=True
        )
    ] = None,
    out_file: Annotated[
        Optional[str],
        typer.Option(
            help="Path to save panel CSV file.If not specified, default auto-generated path *ru-smb-data/panelize/smb/panel.csv* is used",
            show_default="auto-generated in form ru-smb-data/panelize/smb/panel.csv"
        )
    ] = None,
    revexp_file: Annotated[
        Optional[pathlib.Path],
        typer.Option(
            help="Path to aggregated CSV revexp file. Usually the same as *out_file* on aggregate stage with *--source-dataset revexp*. If not specified, default auto-generated path *ru-smb-data/aggregate/revexp/agg.csv* is used",
            show_default="auto-generated in form ru-smb-data/aggregate/revexp/agg.csv",
            exists=True,
            file_okay=True,
            readable=True
        )
    ] = None,
    empl_file: Annotated[
        Optional[pathlib.Path],
        typer.Option(
            help="Path to aggregated CSV empl file. Usually the same as *out_file* on aggregate stage with *--source-dataset empl*. If not specified, default auto-generated path *ru-smb-data/aggregate/empl/agg.csv* is used",
            show_default="auto-generated in form ru-smb-data/aggregate/empl/agg.csv",
            exists=True,
            file_okay=True,
            readable=True
        )
    ] = None,
):
    """
    Make panel dataset based on georeferenced SMB data and aggregated revexp and empl tables (stage 5)
    """
    smb_file = str(smb_file) or get_default_path(StageNames.georeference.value, SourceDatasets.smb.value, filename="georeferenced.csv")
    p = Panelizer()
    p(smb_file, out_file, revexp_file, empl_file)


@app.command(rich_help_panel="Configuration")
def config(
    show: Annotated[
        bool,
        typer.Option("--show", help="Only show current config without updating", show_default="false")
    ] = False,
    ydisk_token: Annotated[
        str,
        typer.Option(help="Token for Yandex Disk; used if *storage* is *ydisk*")
    ] = "",
    num_workers: Annotated[
        int,
        typer.Option(help="Number of workers = processes for extractor")
    ] = 1,
    extractor_chunksize: Annotated[
        int,
        typer.Option(help="Chunk size for extractor")
    ] = 16,
    storage: Annotated[
        Storages,
        typer.Option(help="Place to download source datasets (note: *source datasets only* rather than all other files)")
    ] = Storages.local.value,
):
    """
    Show or set global options for all commands
    """
    if show:
        print("Current configuration")
        for key, value in app_config.items():
            print(key, value)

        return

    app_config["token"] = ydisk_token
    app_config["num_workers"] = extractor_num_workers
    app_config["chunksize"] = extractor_chunksize
    app_config["storage"] = storage

    with open(app_config_path, "w") as f:
        json.dump(app_config, f)

    print("Configuration updated")


@app.command(rich_help_panel="Magic")
def process(
    download: Annotated[
        bool,
        typer.Option(
            help="Download source datasets before processing. If False, the application expects that source datasets have already been downloaded to *ru-smb-data/download/smb*, *ru-smb-data/download/revexp*, and ru-smb-data/download/empl*"
        )
    ] = False,
    ac: Annotated[
        Optional[List[str]],
        typer.Option(
            help="**A**ctivity **c**ode(s) to filter smb source dataset by. Can be either activity group code, e.g. *--ac A*, or exact digit code, e.g. *--ac 01.10*. Multiple codes or groups can be specified by multiple *ac* options, e.g. *--ac 01.10 --ac 69.20*. Top-level codes include child codes, i.e. *--ac 01.10* selects 01.10.01, 01.10.02, 01.10.10 (if any children are present). If not specified, filtering is disabled. If *source_dataset* (see above) is *revexp* or *empl*, filtering is not performed",
            show_default="no filtering by activity code(s)"
        )
    ] = None
):
    """
    Process the source data with this single command
    """
    if download:
        download()

    extract()
    aggregate()
    georeference()
    panelize()


if __name__ == "__main__":
    app()
