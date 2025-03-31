import pathlib
import random
from collections import defaultdict
from functools import partial
from typing import Any, Optional

import pytest
import requests

from .common import MockResponse, mock_get, mock_post, mock_put, mock_ydisk_api
from ..stages.download import Downloader
from ..utils.enums import SourceDatasets, Storages


def test_wrong_storage_type():
    with pytest.raises(RuntimeError, match="storage"):
        downloader = Downloader("s3")


def test_no_token_for_ydisk():
    with pytest.raises(RuntimeError, match="Token"):
        downloader = Downloader(Storages.ydisk.value)


def test_wrong_source_dataset():
    downloader = Downloader(Storages.local.value)

    with pytest.raises(RuntimeError, match="dataset"):
        downloader(source_dataset="egrul")


def test_local_download(monkeypatch, tmp_path):
    downloader = Downloader(Storages.local.value)

    monkeypatch.setattr(requests, "get", mock_get)

    sme_download_dir = tmp_path / "sme"
    downloader(SourceDatasets.sme.value, str(sme_download_dir))

    downloaded_files = list(sme_download_dir.glob("*.zip"))

    assert len(set(downloaded_files)) == 103

    revexp_download_dir = tmp_path / "revexp"
    downloader(SourceDatasets.revexp.value, str(revexp_download_dir))

    downloaded_files = list(revexp_download_dir.glob("*.zip"))

    assert len(downloaded_files) == 1

    empl_download_dir = tmp_path / "empl"
    downloader(SourceDatasets.empl.value, str(empl_download_dir))

    downloaded_files = list(empl_download_dir.glob("*.zip"))

    assert len(downloaded_files) == 1


def test_local_download_no_reload(monkeypatch, tmp_path):
    downloader = Downloader(Storages.local.value)

    monkeypatch.setattr(requests, "get", mock_get)

    sme_download_dir = tmp_path / "sme"
    downloader(SourceDatasets.sme.value, str(sme_download_dir))

    downloaded_files = list(sme_download_dir.glob("*.zip"))

    assert len(set(downloaded_files)) == 103

    # Should not be called
    monkeypatch.setattr(MockResponse, "iter_content", None)
    downloader(SourceDatasets.sme.value, str(sme_download_dir))

    downloaded_files = list(sme_download_dir.glob("*.zip"))

    assert len(set(downloaded_files)) == 103


def test_ydisk_download(monkeypatch, tmp_path):
    downloader = Downloader(Storages.ydisk.value, "token")

    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(requests, "post", mock_post)
    monkeypatch.setattr(requests, "put", mock_put)
    monkeypatch.setattr(downloader, "YDISK_DOWNLOAD_TIMEOUT", 0)

    sme_download_dir = tmp_path / "sme"
    downloader(SourceDatasets.sme.value, str(sme_download_dir))

    downloaded_files = downloader._get_existing_files(str(sme_download_dir))

    assert len(set(downloaded_files)) == 103

    revexp_download_dir = tmp_path / "revexp"
    downloader(SourceDatasets.revexp.value, str(revexp_download_dir))

    downloaded_files = downloader._get_existing_files(str(revexp_download_dir))

    assert len(set(downloaded_files)) == 1

    empl_download_dir = tmp_path / "empl"
    downloader(SourceDatasets.empl.value, str(empl_download_dir))

    downloaded_files = downloader._get_existing_files(str(empl_download_dir))

    assert len(set(downloaded_files)) == 1
