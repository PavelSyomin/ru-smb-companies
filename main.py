import json
import pathlib
from typing import List, Optional

import typer

from stages.aggregate import Aggregator
from stages.download import Downloader
from stages.extract import Extractor
from stages.georeference import Georeferencer
from stages.panelize import Panelizer


APP_NAME = "ru_smb_companies"

app = typer.Typer()

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
    app_config = {}


def ac_list(activity_codes: str) -> List[str]:
    return list(map(str.strip, activity_codes.split(",")))


@app.command()
def download(storage: str = "local", mode: str = "smb", download_dir: Optional[str] = None):
    if storage in ("ydisk", ):
        token = app_config.get("token")
        if token is None:
            raise RuntimeError("Token is required to use ydisk storage")
    else:
        token = None

    d = Downloader(token)
    d(storage, mode, download_dir)


@app.command()
def extract(in_dir: str, out_file: str, mode: str = "smb",
            clear: bool = False, activity_codes: Optional[str] = None, storage: str = "local"):
    activity_codes = ac_list(activity_codes)
    num_workers = app_config.get("num_workers", 1)
    chunksize = app_config.get("chunksize", 16)
    if storage in ("ydisk", ):
        token = app_config.get("token")
        if token is None:
            raise RuntimeError("Token is required to use ydisk storage")
    else:
        token = None

    e = Extractor(storage, num_workers, chunksize, token)
    e(in_dir, out_file, mode, clear, activity_codes)


@app.command()
def aggregate(in_dir: str, out_file: str,
              mode: str = "smb", smb_data_file: Optional[str] = None):
    a = Aggregator()
    a(in_dir, out_file, mode, smb_data_file)


@app.command()
def georeference(in_file: str, out_file: str):
    g = Georeferencer()
    d(in_file, out_file)


@app.command()
def panelize(smb_file: str, out_file: str,
          revexp_file: Optional[str] = None,
          empl_file: Optional[str] = None):
    p = Panelizer()
    p(smb_file, out_file, revexp_file, empl_file)


@app.command()
def config(
    show: Annotated[bool, typer.Option("--show")] = True,
    ydisk_token: Optional[str] = None,
    extractor_num_workers: int = 1,
    extractor_chunksize: int = 16
):
    app_config["token"] = ydisk_token
    app_config["num_workers"] = extractor_num_workers
    app_config["chunksize"] = extractor_chunksize

    with open(app_config_path, "w") as f:
        json.dump(app_config, f)

    print("Configuration updated")


@app.command()
def process(activity_codes: str = ""):
    # Specify various paths
    data_path = pathlib.Path("data")
    raw_data_path = data_path / "raw"
    proc_data_path = data_path / "proc"
    result_data_path = data_path / "result"

    # Download stage (1) is omitted

    for mode in ("smb", "revexp", "empl"):
        # Extract stage (2)
        in_dir = raw_data_path / mode
        out_dir = proc_data_path / mode
        if in_dir.exists():
            extract(in_dir, out_dir, mode, clean=False, activity_codes=activity_codes)

        # Aggregate stage (3)
        in_dir = out_dir
        out_file = proc_data_path / mode / "agg.csv"
        if in_dir.exists():
            if mode == "smb":
                aggregate(in_dir, out_file, mode)
            else:
                aggregate(in_dir, out_file, mode, proc_data_path / "smb" / "agg.csv")

    # Georeference stage (4)
    georeference(proc_data_path / "smb" / "agg.csv", result_data_path / "smb.csv")

    # Make panel (5)
    panelize(
        result_data_path / "smb.csv",
        result_data_path / "panel.csv",
        proc_data_path / "revexp" / "agg.csv",
        proc_data_path / "empl" / "agg.csv"
    )

    shutil.copy(proc_data_path / "revexp" / "agg.csv", result_data_path / "revexp.csv")
    shutil.copy(proc_data_path / "empl" / "agg.csv", result_data_path / "empl.csv")


@app.command()
def download_and_process(activity_codes: str = ""):
    # Specify various paths
    data_path = pathlib.Path("data")
    raw_data_path = data_path / "raw"

    for mode in ("smb", "revexp", "empl"):
        download("local", mode, raw_data_path / mode)

    process(activity_codes)


if __name__ == "__main__":
    app()
