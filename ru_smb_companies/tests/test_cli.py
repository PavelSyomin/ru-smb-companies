import pathlib
import shutil

import pandas as pd
import pytest
import requests
from typer.testing import CliRunner

from ru_smb_companies.main import app
from ru_smb_companies.utils.enums import SourceDatasets
from .common import mock_get

runner = CliRunner()


def test_help():
    """Test help command works"""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.stdout


def test_download_all(monkeypatch, tmp_path):
    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["download", "all"])

    assert result.exit_code == 0

    download_subdirs = list((tmp_path / "ru-smb-data" / "download").iterdir())
    assert len(download_subdirs) == 3
    assert sorted(d.name for d in download_subdirs) == ["empl", "revexp", "smb"]

    smb_files = list((tmp_path / "ru-smb-data" / "download" / "smb").iterdir())
    assert len(smb_files) > 1
    assert smb_files[0].name.startswith("data-")
    assert smb_files[0].name.endswith(".zip")

    revexp_files = list((tmp_path / "ru-smb-data" / "download" / "revexp").iterdir())
    assert len(revexp_files) == 1
    assert revexp_files[0].name.startswith("data-")
    assert revexp_files[0].name.endswith(".zip")

    empl_files = list((tmp_path / "ru-smb-data" / "download" / "empl").iterdir())
    assert len(empl_files) == 1
    assert empl_files[0].name.startswith("data-")
    assert empl_files[0].name.endswith(".zip")


def test_download_all_custom_dir(monkeypatch, tmp_path):
    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["download", "all", "--download-dir", "custom-dir"])

    assert result.exit_code == 0

    download_subdirs = list((tmp_path / "custom-dir").iterdir())
    assert len(download_subdirs) == 3
    assert sorted(d.name for d in download_subdirs) == ["empl", "revexp", "smb"]


def test_extract_all(monkeypatch, tmp_path):
    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.chdir(tmp_path)

    tests_data_dir = pathlib.Path(__file__).parent / "data"
    for source_dataset in SourceDatasets:
        for f in tests_data_dir.glob(f"{source_dataset.value}/*.zip"):
            target_dir = tmp_path / "ru-smb-data" / "download" / source_dataset.value
            target_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy(f, target_dir / f.name)

    result = runner.invoke(app, ["extract", "all"])

    assert result.exit_code == 0

    assert len(list((tmp_path / "ru-smb-data" / "extract").iterdir())) == 3
    assert len(list((tmp_path / "ru-smb-data" / "extract" / "smb").glob("*.csv"))) == 2
    assert len(list((tmp_path / "ru-smb-data" / "extract" / "revexp").glob("*.csv"))) == 2
    assert len(list((tmp_path / "ru-smb-data" / "extract" / "empl").glob("*.csv"))) == 2


def test_extract_all_custom_dirs(monkeypatch, tmp_path):
    monkeypatch.setattr(requests, "get", mock_get)

    args = ["extract", "all"]
    args.extend(["--in-dir", str(pathlib.Path(__file__).parent / "data")])
    args.extend(["--out-dir", str(tmp_path / "results")])

    result = runner.invoke(app, args)

    assert result.exit_code == 0

    assert len(list((tmp_path / "results").iterdir())) == 3
    assert len(list((tmp_path / "results" / "smb").glob("*.csv"))) == 2
    assert len(list((tmp_path / "results" / "revexp").glob("*.csv"))) == 2
    assert len(list((tmp_path / "results" / "empl").glob("*.csv"))) == 2


def test_extract_all_clear_out_dir(monkeypatch, tmp_path):
    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr("builtins.input", lambda prompt: "yes")

    for option in ("smb", "revexp", "empl"):
        dir = tmp_path / "results" / option
        dir.mkdir(parents=True)
        for j in range(100):
            (dir / f"file-{j}.csv").touch()

    args = ["extract", "all"]
    args.extend(["--in-dir", str(pathlib.Path(__file__).parent / "data")])
    args.extend(["--out-dir", str(tmp_path / "results")])
    args.extend(["--clear"])
    result = runner.invoke(app, args)

    assert result.exit_code == 0

    assert len(list((tmp_path / "results").iterdir())) == 3

    # All generated empty CSV files should be removed by --clear
    assert len(list((tmp_path / "results" / "smb").glob("*.csv"))) == 2
    assert len(list((tmp_path / "results" / "revexp").glob("*.csv"))) == 2
    assert len(list((tmp_path / "results" / "empl").glob("*.csv"))) == 2


def test_extract_all_filter_by_activity_code(monkeypatch, tmp_path):
    monkeypatch.setattr(requests, "get", mock_get)

    out_dir = tmp_path / "results"

    args = ["extract", "all"]
    args.extend(["--in-dir", str(pathlib.Path(__file__).parent / "data")])
    args.extend(["--out-dir", str(out_dir)])
    args.extend(["--ac", "47", "--ac", "49"])

    result = runner.invoke(app, args)

    assert result.exit_code == 0

    assert len(list(out_dir.iterdir())) == 3

    extracted = pd.read_csv(out_dir / "smb" / "smb-test-data-1.csv", dtype=str)
    assert all(
        c.startswith("47") or c.startswith("49")
        for c in extracted["activity_code_main"].unique()
    )
    assert len(extracted.loc[extracted["ind_tin"] == "523102417490"]) == 1
    assert len(extracted.loc[extracted["ind_tin"] == "523400533580"]) == 1


def test_process_no_data(monkeypatch, tmp_path):
    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["process"])

    assert result.exit_code == 0





