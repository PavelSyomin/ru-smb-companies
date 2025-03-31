import pathlib
import random
from collections import defaultdict
from functools import partial
from typing import Any, Optional


class MockResponse:
    def __init__(
        self,
        status_code: int = 200,
        json: Any = None,
        text: Optional[str] = None,
        file_path: str = None
    ):
        self._status_code = status_code
        self._json = json
        self._text = text
        self._file_path = file_path

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
        if self._file_path is not None:
            real_file_path = pathlib.Path(
                pathlib.Path(__file__).parent / self._file_path
            )

            if real_file_path.exists():
                with open(real_file_path, "rb") as f:
                    for block in iter(partial(f.read, 2**10), b""):
                        yield block

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
            json={"href": f"operations/{random.randint(1, int(1e6))}"}
        )

    def get(self, url, headers, params):
        self._authorize(headers)

        if self.API_ENDPOINTS["resources"] in url:
            path = params.get("path")

            if not path:
                return MockResponse(status_code=400)

            if "download" in url:
                path = path.replace("russian-sme-registry-data/download", "")
                return MockResponse(json={"href": f"data/{path}"})
            else:
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
    
    def clear(self):
        self._storage = defaultdict(dict)


def mock_get(url: str, headers: dict = {}, params: dict = {}, **kwargs):
    if "data" in url and "zip" in url:
        resp = MockResponse(file_path=url)
    elif "rsmp" in url:
        with open(pathlib.Path(__file__).parent / "data/sme/sme.html") as f:
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
    elif any(p in url for p in ["disk/resources", "operations"]):
        resp = mock_ydisk_api.get(url, headers, params)
    else:
        resp = MockResponse(404)

    return resp


def mock_put(url: str, **kwargs):
    return mock_ydisk_api.put(url, **kwargs)


def mock_post(url: str, **kwargs):
    return mock_ydisk_api.post(url, **kwargs)


mock_ydisk_api = MockYdiskAPI()
