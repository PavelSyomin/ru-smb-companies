import json
import pathlib
import zipfile

import pandas as pd
import pytest
import requests

from .common import mock_get, mock_ydisk_api
from ..stages.extract import Extractor
from ..utils.elements import elements
from ..utils.enums import SourceDatasets, Storages


def test_empty_input_directory(tmp_path):
    in_dir = tmp_path / "input_data"
    out_dir = tmp_path / "results"
    in_dir.mkdir()
    out_dir.mkdir()

    extractor = Extractor(Storages.local.value)

    call_result = extractor(str(in_dir), str(out_dir), SourceDatasets.sme.value)

    assert call_result is None


def test_no_zip_files_in_input_directory(tmp_path):
    in_dir = tmp_path / "input_data"
    out_dir = tmp_path / "results"
    in_dir.mkdir()
    out_dir.mkdir()

    with open(in_dir / "sample_input_file.xml", "w") as f:
        f.write("<test></test>")

    extractor = Extractor(Storages.local.value)

    call_result = extractor(str(in_dir), str(out_dir), SourceDatasets.sme.value)

    assert call_result is None


def test_clear_out_dir(monkeypatch, tmp_path):
    in_dir = tmp_path / "input_data"
    out_dir = tmp_path / "results"
    in_dir.mkdir()
    out_dir.mkdir()

    with zipfile.ZipFile(in_dir / "sample_input_file.zip", "w") as zf:
        zf.writestr("test.txt", "test_content")

    for i in range(10):
        with open(out_dir / f"file_{i}.csv", "w") as f:
            f.write(f"File {i}")

    extractor = Extractor(Storages.local.value)

    monkeypatch.setattr("builtins.input", lambda prompt: "yes")

    call_result = extractor(
        str(in_dir), str(out_dir), SourceDatasets.sme.value, clear=True
    )

    assert input("prompt") == "yes"
    assert call_result == 1
    assert len(list(out_dir.iterdir())) == 1 # only history.json


def test_wrong_storage_type():
    with pytest.raises(RuntimeError, match="storage"):
        extractor = Extractor("s3")


def test_no_token_for_ydisk():
    with pytest.raises(RuntimeError, match="Token"):
        extractor = Extractor(Storages.ydisk.value)


def test_record_history(tmp_path):
    in_dir = tmp_path / "input_data"
    out_dir = tmp_path / "results"
    in_dir.mkdir()
    out_dir.mkdir()

    for i in range(10):
        with zipfile.ZipFile(in_dir / f"sample_input_file_{i}.zip", "w") as zf:
            zf.writestr("test.txt", "test_content")

    extractor = Extractor(Storages.local.value)

    call_result = extractor(str(in_dir), str(out_dir), SourceDatasets.sme.value)

    assert call_result == 10
    assert len(list(out_dir.iterdir())) == 1 # only history.json

    with open(out_dir / "history.json") as f:
        assert len(json.load(f)) == 10


def test_use_history(tmp_path):
    in_dir = tmp_path / "input_data"
    out_dir = tmp_path / "results"
    in_dir.mkdir()
    out_dir.mkdir()

    for i in range(5):
        with zipfile.ZipFile(in_dir / f"sample_input_file_{i}.zip", "w") as zf:
            zf.writestr("test.txt", "test_content")

    extractor = Extractor(Storages.local.value)

    call_result = extractor(str(in_dir), str(out_dir), SourceDatasets.sme.value)

    assert call_result == 5
    assert len(list(out_dir.iterdir())) == 1 # only history.json

    with open(out_dir / "history.json") as f:
        assert len(json.load(f)) == 5

    for i in range(5, 10):
        with zipfile.ZipFile(in_dir / f"sample_input_file_{i}.zip", "w") as zf:
            zf.writestr("test.txt", "test_content")

    call_result = extractor(str(in_dir), str(out_dir), SourceDatasets.sme.value)

    assert call_result == 5
    assert len(list(out_dir.iterdir())) == 1 # only history.json

    with open(out_dir / "history.json") as f:
        assert len(json.load(f)) == 10

    call_result = extractor(str(in_dir), str(out_dir), SourceDatasets.sme.value)
    assert call_result == 0


def test_extract_sme(tmp_path):
    in_dir = pathlib.Path(__file__).parent / "data/sme"
    out_dir = tmp_path / "results"
    out_dir.mkdir()

    extractor = Extractor(Storages.local.value)

    call_result = extractor(str(in_dir), str(out_dir), SourceDatasets.sme.value)

    assert call_result == 2
    assert len(list(out_dir.glob("*.csv"))) == 2

    extracted = pd.read_csv(out_dir / "data-test-sme-1.csv", dtype=str)
    assert len(extracted) > 0
    assert (
        sorted(extracted.columns)
        == sorted(list(elements["sme"].values()) + ["file_id", "doc_cnt"])
    )

    sample_row = extracted.loc[extracted["ind_tin"] == "523400993387"].iloc[0]

    assert sample_row["kind"] == "2"
    assert sample_row["category"] == "1"
    assert sample_row["reestr_date"] == "10.08.2018"
    assert sample_row["data_date"] == "10.02.2019"
    assert sample_row["ind_tin"] == "523400993387"
    assert pd.isna(sample_row["ind_number"])
    assert sample_row["first_name"] == "АНАСТАСИЯ"
    assert sample_row["last_name"] == "НОВОСЕЛОВА"
    assert sample_row["patronymic"] == "НИКОЛАЕВНА"
    assert pd.isna(sample_row["org_name"])
    assert pd.isna(sample_row["org_short_name"])
    assert pd.isna(sample_row["org_tin"])
    assert sample_row["region_code"] == "52"
    assert sample_row["region_name"] == "НИЖЕГОРОДСКАЯ"
    assert sample_row["region_type"] == "ОБЛАСТЬ"
    assert sample_row["district_name"] == "ТОНШАЕВСКИЙ"
    assert sample_row["district_type"] == "РАЙОН"
    assert pd.isna(sample_row["city_name"])
    assert pd.isna(sample_row["city_type"])
    assert sample_row["settlement_name"] == "БУРЕПОЛОМ"
    assert sample_row["settlement_type"] == "ПОСЕЛОК"
    assert sample_row["activity_code_main"] == "96.02"

    sample_row = extracted.loc[extracted["org_tin"] == "5234003180"].iloc[0]

    assert sample_row["kind"] == "1"
    assert sample_row["category"] == "1"
    assert sample_row["reestr_date"] == "01.08.2016"
    assert sample_row["data_date"] == "10.02.2019"
    assert pd.isna(sample_row["ind_tin"])
    assert pd.isna(sample_row["ind_number"])
    assert pd.isna(sample_row["first_name"])
    assert pd.isna(sample_row["last_name"])
    assert pd.isna(sample_row["patronymic"])
    assert sample_row["org_name"] == 'ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "ПИЖМАЛЕС"'
    assert sample_row["org_short_name"] == 'ООО "ПИЖМАЛЕС"'
    assert sample_row["region_code"] == "52"
    assert sample_row["region_name"] == "НИЖЕГОРОДСКАЯ"
    assert sample_row["region_type"] == "ОБЛАСТЬ"
    assert sample_row["district_name"] == "ТОНШАЕВСКИЙ"
    assert sample_row["district_type"] == "РАЙОН"
    assert pd.isna(sample_row["city_name"])
    assert pd.isna(sample_row["city_type"])
    assert sample_row["settlement_name"] == "ПИЖМА"
    assert sample_row["settlement_type"] == "РАБОЧИЙ ПОСЕЛОК"
    assert sample_row["activity_code_main"] == "46.73.1"


def test_extract_revexp(tmp_path):
    in_dir = pathlib.Path(__file__).parent / "data/revexp"
    out_dir = tmp_path / "results"
    out_dir.mkdir()

    extractor = Extractor(Storages.local.value)

    call_result = extractor(
        str(in_dir), str(out_dir), SourceDatasets.revexp.value
    )

    assert call_result == 2
    assert len(list(out_dir.glob("*.csv"))) == 2

    extracted = pd.read_csv(out_dir / "data-test-revexp-1.csv", dtype=str)
    assert len(extracted) > 0
    assert (
        sorted(extracted.columns)
        == sorted(elements["revexp"].values())
    )

    sample_row = extracted.loc[extracted["org_tin"] == "5405343781"].iloc[0]

    assert sample_row["revenue"] == "43949000.00"
    assert sample_row["expenditure"] == "3426000.00"
    assert sample_row["data_date"] == "31.12.2018"
    assert sample_row["doc_date"] == "15.10.2019"


def test_extract_empl(tmp_path):
    in_dir = pathlib.Path(__file__).parent / "data/empl"
    out_dir = tmp_path / "results"
    out_dir.mkdir()

    extractor = Extractor(Storages.local.value)

    call_result = extractor(
        str(in_dir), str(out_dir), SourceDatasets.empl.value
    )

    assert call_result == 2
    assert len(list(out_dir.glob("*.csv"))) == 2

    extracted = pd.read_csv(out_dir / "data-test-empl-1.csv", dtype=str)
    assert len(extracted) > 0
    assert (
        sorted(extracted.columns)
        == sorted(elements["empl"].values())
    )

    sample_row = extracted.loc[extracted["org_tin"] == "7743860009"].iloc[0]

    assert sample_row["employees_count"] == "2"
    assert sample_row["data_date"] == "31.12.2018"
    assert sample_row["doc_date"] == "31.03.2020"


def test_extract_sme_filter_by_activity_code(tmp_path):
    in_dir = pathlib.Path(__file__).parent / "data/sme"
    out_dir = tmp_path / "results"
    out_dir.mkdir()

    extractor = Extractor(Storages.local.value)

    # Single code
    call_result = extractor(
        str(in_dir),
        str(out_dir),
        SourceDatasets.sme.value,
        activity_codes=["47"],
    )

    assert call_result == 2
    assert len(list(out_dir.glob("*.csv"))) == 2

    extracted = pd.read_csv(out_dir / "data-test-sme-1.csv", dtype=str)
    assert len(extracted) > 0
    assert (
        sorted(extracted.columns)
        == sorted(list(elements["sme"].values()) + ["file_id", "doc_cnt"])
    )

    assert all(
        c.startswith("47") for c in extracted["activity_code_main"].unique()
    )

    assert len(extracted.loc[extracted["ind_tin"] == "523102417490"]) == 1
    assert len(extracted.loc[extracted["ind_tin"] == "523400533580"]) == 0

    for f in out_dir.iterdir():
        f.unlink()

    # Multiple codes
    call_result = extractor(
        str(in_dir),
        str(out_dir),
        SourceDatasets.sme.value,
        activity_codes=["47", "49"],
    )

    extracted = pd.read_csv(out_dir / "data-test-sme-1.csv", dtype=str)
    assert all(
        c.startswith("47") or c.startswith("49")
        for c in extracted["activity_code_main"].unique()
    )
    assert len(extracted.loc[extracted["ind_tin"] == "523102417490"]) == 1
    assert len(extracted.loc[extracted["ind_tin"] == "523400533580"]) == 1

    for f in out_dir.iterdir():
        f.unlink()

    # Entire group
    call_result = extractor(
        str(in_dir),
        str(out_dir),
        SourceDatasets.sme.value,
        activity_codes=["C"], # latin C rather than cyrillic :)
    )

    extracted = pd.read_csv(out_dir / "data-test-sme-1.csv", dtype=str)
    assert all(
        int(c.split(".")[0]) in range(10, 34)
        for c in extracted["activity_code_main"].unique()
    )
    assert len(extracted.loc[extracted["ind_tin"] == "523400390188"]) == 1
    assert len(extracted.loc[extracted["ind_tin"] == "523300890660"]) == 0


def test_extract_ydisk(monkeypatch, tmp_path):
    in_dir = "sme"
    out_dir = tmp_path / "results"
    out_dir.mkdir()

    extractor = Extractor(Storages.ydisk.value, token="token")

    mock_ydisk_api.put(
        "disk/resources",
        dict(Authorization=f"OAuth token"),
        dict(path="sme"),
    )

    data_dir = pathlib.Path(__file__).parent / "data" / "sme"
    for f in data_dir.glob("*.zip"):
        mock_ydisk_api.post(
            "disk/resources/upload",
            dict(Authorization=f"OAuth token"),
            dict(path=f"sme/{f.name}", url=f.name),
        )
    monkeypatch.setattr(requests, "get", mock_get)

    call_result = extractor(
        in_dir,
        str(out_dir),
        SourceDatasets.sme.value,
    )

    assert call_result == 2
    assert len(list(out_dir.glob("*.csv"))) == 2
