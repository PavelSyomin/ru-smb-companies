import pathlib
import random
from collections import defaultdict
from typing import Any, Optional

import pytest
import requests

from ..stages.download import Downloader
from ..utils.enums import SourceDatasets, Storages


class MockResponse:
    def __init__(
        self,
        status_code: int = 200,
        json: Any = None,
        text: Optional[str] = None
    ):
        self._status_code = status_code
        self._json = json
        self._text = text

    @property
    def status_code(self):
        return self._status_code

    @property
    def text(self):
        if self._text is not None:
            return self._text
        elif self._json is not None:
            return str(self._json)
        else:
            return ""

    def json(self):
        return self._json or {}

    def iter_content(self, *args, **kwargs):
        for i in range(10):
            yield f"chunk {i}".encode()


class MockYdiskAPI:
    API_ENDPOINTS = {
        "resources": "disk/resources",
        "upload": "disk/resources/upload",
    }
    TOKEN = "token"

    def __init__(self):
        self._storage = defaultdict(dict)

    def _authorize(self, headers: dict):
        if headers.get("Authorization") != f"OAuth {self.TOKEN}":
            return MockResponse(status_code=401)

    def post(self, url, headers, params):
        self._authorize(headers)

        if self.API_ENDPOINTS["upload"] not in url:
            return MockResponse(status_code=404)

        path = params.get("path")
        file_url = params.get("url")
        if not (path and file_url):
            return MockResponse(status_code=400)

        parts = list(filter(None, path.split("/")))
        folder = self._storage
        for part in parts[:-1]:
            folder = folder.get(part)
            if not folder:
                return MockResponse(status_code=404)

        folder["items"].append(parts[-1])

        return MockResponse(
            status_code=202,
            json={"href": f"operations/{random.randint(1, 1e6)}"}
        )

    def get(self, url, headers, params):
        self._authorize(headers)

        if self.API_ENDPOINTS["resources"] in url:
            path = params.get("path")

            if not path:
                return MockResponse(status_code=400)

            parts = list(filter(None, path.split("/")))
            folder = self._storage
            for part in parts:
                folder = folder.get(part)
                if folder is None:
                    return MockResponse(status_code=404)

            files = [
                {"type": "file", "path": fn}
                for fn in folder.get("items", [])
            ]

            return MockResponse(json={"_embedded": {"items": files}})
        elif "operations" in url:
            return MockResponse(json={"status": "success"})

        return MockResponse(status_code=404)

    def put(self, url, headers, params):
        self._authorize(headers)

        if self.API_ENDPOINTS["resources"] not in url:
            return MockResponse(status_code=404)

        path = params.get("path")

        if not path:
            return MockResponse(status_code=400)

        parts = list(filter(None, path.split("/")))
        if len(parts) == 1:
            self._storage[parts[0]]["items"] = []

            return MockResponse(status_code=201)

        folder = self._storage
        for part in parts[:-1]:
            folder = folder.get(part)
            if folder is None:
                return MockResponse(status_code=404)

        if parts[-1] in folder:
            return MockResponse(
                status_code=409,
                json={"error": "DiskPathPointsToExistentDirectoryError"}
            )

        folder[parts[-1]] = {"items": []}

        return MockResponse(status_code=201)


def mock_get(url: str, headers: dict = {}, params: dict = {}, **kwargs):
    if "data" in url and "zip" in url:
        resp = MockResponse()
    elif "rsmp" in url:
        with open(pathlib.Path(__file__).parent / "data/smb/smb.html") as f:
            text = f.read()
        resp = MockResponse(text=text)
    elif "revexp" in url:
        with open(pathlib.Path(__file__).parent / "data/revexp/revexp.html") as f:
            text = f.read()
        resp = MockResponse(text=text)
    elif "sshr" in url:
        with open(pathlib.Path(__file__).parent / "data/empl/empl.html") as f:
            text = f.read()
        resp = MockResponse(text=text)
    elif "disk/resources" in url or "operations" in url:
        resp = mock_ydisk_api.get(url, headers, params)
    else:
        resp = MockResponse(404)

    return resp


def mock_put(url: str, **kwargs):
    return mock_ydisk_api.put(url, **kwargs)


def mock_post(url: str, **kwargs):
    return mock_ydisk_api.post(url, **kwargs)


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

    smb_download_dir = tmp_path / "smb"
    downloader(SourceDatasets.smb.value, str(smb_download_dir))

    downloaded_files = list(smb_download_dir.glob("*.zip"))

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

    smb_download_dir = tmp_path / "smb"
    downloader(SourceDatasets.smb.value, str(smb_download_dir))

    downloaded_files = list(smb_download_dir.glob("*.zip"))

    assert len(set(downloaded_files)) == 103

    # Should not be called
    monkeypatch.setattr(MockResponse, "iter_content", None)
    downloader(SourceDatasets.smb.value, str(smb_download_dir))

    downloaded_files = list(smb_download_dir.glob("*.zip"))

    assert len(set(downloaded_files)) == 103

mock_ydisk_api = MockYdiskAPI()

def test_ydisk_download(monkeypatch, tmp_path):
    downloader = Downloader(Storages.ydisk.value, "token")

    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(requests, "post", mock_post)
    monkeypatch.setattr(requests, "put", mock_put)
    monkeypatch.setattr(downloader, "YDISK_DOWNLOAD_TIMEOUT", 0)

    smb_download_dir = tmp_path / "smb"
    downloader(SourceDatasets.smb.value, str(smb_download_dir))

    downloaded_files = downloader._get_existing_files(str(smb_download_dir))

    assert len(set(downloaded_files)) == 103

    revexp_download_dir = tmp_path / "revexp"
    downloader(SourceDatasets.revexp.value, str(revexp_download_dir))

    downloaded_files = downloader._get_existing_files(str(revexp_download_dir))

    assert len(set(downloaded_files)) == 1

    empl_download_dir = tmp_path / "empl"
    downloader(SourceDatasets.empl.value, str(empl_download_dir))

    downloaded_files = downloader._get_existing_files(str(empl_download_dir))

    assert len(set(downloaded_files)) == 1
