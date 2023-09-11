from collections import namedtuple
import functools
import json
import multiprocessing
import pathlib
import sys
import time
from typing import List
from urllib.parse import urljoin
import zipfile

import lxml.etree as ET
import pandas as pd
import requests
from tqdm import tqdm


"""Params for preprocessor
data_path: str = path to folder with input ZIP archives
xsl_path: str = path to a file with xsl stylesheet for transforming original XML files from data_path
    to their flattened suitable for Pandas read_xml method views
out_path: str = path to a local folder where to store produced CSV file
data_source: str = location of data_path ('local' or 'ydisk')
clear: bool = remove existing CSV file and folder
"""
Config = namedtuple(
    "Config",
    ["data_path", "xsl_path", "out_path", "data_source", "clear", "num_workers", "chunksize", "token"],
    defaults=["", "", "", "local", False, 4, 16, ""])


def make_dataframe(item, xsl_path):
    if len(item) != 2:
        print("make_dataframe function expects filename and its content as a [str, str] tuple")
        return None

    fn, xml_string = item
    try:
        df = pd.read_xml(xml_string, stylesheet=xsl_path, dtype=str)
        df.dropna(how="all", inplace=True)
        return df
    except ET.XMLSyntaxError as e:
        try:
            xslt = ET.parse(xsl_path)
            transformer = ET.XSLT(xslt)
            p = ET.XMLParser(huge_tree=True)
            xml = ET.fromstring(xml_string.encode("utf8"), parser=p)
            xml_string_converted = str(transformer(xml))
            df = pd.read_xml(xml_string_converted, dtype=str)
            df.dropna(how="all", inplace=True)
            return df
        except Exception as e:
            print(f"Something is wrong with {e}, skipping")
            print(e)

    return None


class Archive:
    def __init__(self, path, start=None, stop=None, step=None):
        self._archive = zipfile.ZipFile(path)
        self._path = path
        self._xml_list = [fn for fn in self._archive.namelist() if "xml" in fn][start:stop:step]
        self._xml_iterable = iter(self._xml_list)

    def __del__(self):
        self._archive.close()

    def __len__(self):
        return len(self._xml_list)

    def __next__(self):
        fn = next(self._xml_iterable)
        return fn, self._read(fn)

    def __iter__(self):
        for fn in iter(self._xml_list):
            yield fn, self._read(fn)

    def __getitem__(self, index):
        if isinstance(index, int):
            fn = self._xml_list[index]
            return fn, self._read(fn)
        elif isinstance(index, slice):
            return Archive(self._path, index.start, index.stop, index.step)
        else:
            raise IndexError("Index for archive must be either int or slice")

    def _read(self, fn):
        return self._archive.read(fn).decode()

class Preprocessor:
    HOST = "https://cloud-api.yandex.net/v1/"

    def __init__(self, config: Config):
        self._data_path = config.data_path
        self._xsl_path = config.xsl_path
        self._out_path = config.out_path
        self._data_source = config.data_source
        self._clear = config.clear
        self._num_workers = config.num_workers
        self._chunksize = config.chunksize
        self._token = config.token

        self._check_config()

        self._make_out_folder()

        self._history_file_path = pathlib.Path(self._out_path) / "history.json"
        self._history = self._get_history()

    def make_csv(self):
        input_files = self._get_files()

        print(f"Found {len(input_files)} ZIP archives in data folder")

        func = functools.partial(make_dataframe, xsl_path=self._xsl_path)
        for filename in input_files:
            if filename in self._history:
                print(f"{filename} already processed")
                continue

            path = self._resolve_local_file_path(filename)
            print(f"Processing {path}")
            out_file = pathlib.Path(self._out_path) / f"{path.stem}.csv"


            st = time.time()
            archive = Archive(path)

            with multiprocessing.Pool(processes=self._num_workers) as pool:
                for df in pool.imap(func, archive, chunksize=self._chunksize):
                    if df is None:
                        continue
                    if out_file.exists():
                        df.to_csv(out_file, index=False, header=False, mode="a")
                    else:
                        df.to_csv(out_file, index=False)

            et = time.time()
            duration = et - st
            print(f"Completed in {duration:.2f}s")

            self._remove_local_file(path)
            self._history.append(filename)
            self._dump_history()
            del archive

    def _check_config(self):
        if not pathlib.Path(self._xsl_path).exists():
            raise RuntimeError("XSL file does not exist")

        if self._data_source not in ("local", "ydisk"):
            raise ValueError("Data source must be either 'local' or 'ydisk'")

    def _download(self, filename: str) -> str:
        print("Downloading file from Yandex Disk to /tmp")

        api_path = "disk/resources/download"
        headers = {
            "Accept": "application/json",
            "Authorization": f"OAuth {self.TOKEN}",
            "Content-Type": "application/json",
        }
        params = {
            "path": self._data_path + "/" + filename,

        }
        url = urljoin(self.HOST, api_path)

        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            print("Cannot get download URL, see error message below")
            print(resp.json())
            return None

        download_url = resp.json().get("href")
        resp = requests.get(download_url, headers=headers, stream=True)
        if resp.status_code != 200:
            print("Cannot download file")
            return None

        out_file = pathlib.Path("/tmp") / filename
        with open(out_file, "wb") as f:
            for chunk in tqdm(resp.iter_content(2**20)): # chunk size is 1 Mib
                f.write(chunk)

        return out_file

    def _dump_history(self):
        with open(self._history_file_path, "w") as f:
            json.dump(self._history, f)

    def _get_files(self) -> List[str]:
        if self._data_source == "local":
            data_folder = pathlib.Path(self._data_path)
            files = [f.name for f in data_folder.glob("*.zip")]
        else:
            files = self._get_files_list_from_ydisk()

        return files

    def _get_files_list_from_ydisk(self) -> List[str]:
        print(f"Getting files list for {self._data_path} on Yandex Disk")

        result = []
        api_path = "disk/resources"
        headers = {
            "Accept": "application/json",
            "Authorization": f"OAuth {self.TOKEN}",
            "Content-Type": "application/json",
        }
        params = {
            "path": self._data_path,
            "fields": "_embedded.items.path,_embedded.items.type",
            "limit": 1000,
        }
        url = urljoin(self.HOST, api_path)

        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            print("Cannot get path medatata, see error message below")
            print(resp.json())
            return result

        for item in resp.json().get("_embedded", {}).get("items", []):
            if item.get("type") == "file":
                _, _, fn = str(item.get("path")).rpartition("/")
                result.append(fn)

        return result

    def _get_history(self):
        if self._history_file_path.exists():
            with open(self._history_file_path) as f:
                history = json.load(f)
        else:
            history = []

        return history

    def _make_out_folder(self):
        out_path = pathlib.Path(self._out_path)

        if out_path.exists():
            if self._clear:
                confirmation = input(
                    f"Going to remove all files in destination folder ({str(out_path)}). "
                    "Type 'yes' (without quotes) to continue: "
                )
                if confirmation != "yes":
                    print("Aborting")
                    sys.exit(0)
                for f in out_path.iterdir():
                    f.unlink()
        else:
            out_path.mkdir(parents=True)

    def _resolve_local_file_path(self, filename: str) -> str:
        if self._data_source == "local":
            file_path = pathlib.Path(self._data_path) / filename
        else:
            file_path = self._download(filename)

        return file_path

    def _remove_local_file(self, path: str):
        if self._data_source == "local":
            return

        local_file = pathlib.Path(path)
        if not local_file.exists():
            return

        local_file.unlink()
        print(f"Local copy of downloaded file at {path} removed")


def main():
    config = Config(
        "sshr/xml", "sshr.xsl", "sshr/csv", "local", True, 4, 32, None)
    p = Preprocessor(config)
    p.make_csv()


if __name__ == "__main__":
    main()
