from collections import namedtuple
import functools
import json
import logging
import multiprocessing
import pathlib
import string
import sys
import tempfile
import time
from typing import List, Optional
from urllib.parse import urljoin
import zipfile

from lxml import etree
import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

from utils.elements import elements


logging.basicConfig(encoding='utf-8', level=logging.INFO)


def _make_dataframe(item, elements, target_codes=None, debug=True):
    if len(item) != 2:
        print("make_dataframe function expects filename and its content as a [str, str] tuple")
        return None

    fn, xml_string = item
    try:
        parser = etree.XMLParser(huge_tree=True)
        root = etree.fromstring(xml_string, parser=parser)
        rows = []

        for doc in root.iter("Документ"):
            if target_codes is not None:
                code = doc.xpath("string(СвОКВЭД/СвОКВЭДОсн/@КодОКВЭД)")
                if code not in target_codes:
                    continue

            row = dict.fromkeys(elements.values())
            for path, key in elements.items():
                matches = doc.xpath(path)
                if len(matches) == 0:
                    continue
                elif len(matches) == 1:
                    row[key] = matches[0]
                else:
                    row[key] = ",".join(matches)
            rows.append(row)

        df = pd.DataFrame(rows)

        if debug:
            df["file_id"] = root.get("ИдФайл")
            df["doc_cnt"] = root.get("КолДок")

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
        return self._archive.read(fn)


class Extractor:
    DATA_SOURCES = ("local", "ydisk")
    HOST = "https://cloud-api.yandex.net/v1/"
    MODES = ("smb", "revexp", "empl")
    ACTIVITY_CODES_CLASSIFIER = "assets/activity_codes_classifier.csv"

    def __init__(self, data_source: str = "local",
                 num_workers: int = 1, chunksize: int = 16,
                 token: Optional[str] = None):
        if data_source not in self.DATA_SOURCES:
            raise ValueError(
                f"Unknown data source {data_source}, "
                f"expected one of {self.DATA_SOURCES}"
            )
        self._data_source = data_source
        self._num_workers = num_workers
        self._chunksize = chunksize
        self._token = token
        self._temp_dir = None

        if data_source in ("ydisk",):
            self._temp_dir = tempfile.TemporaryDirectory()

    def __call__(self, in_dir: str, out_dir: str, mode: str,
                 clear: Optional[bool] = False,
                 activity_codes: Optional[List[str]] = None):
        input_files = self._get_files(in_dir)
        history_file_path = pathlib.Path(out_dir) / "history.json"
        history = self._get_history(history_file_path)
        self._make_out_folder(out_dir, clear)

        if activity_codes is None:
            activity_codes = []

        print(f"Found {len(input_files)} ZIP archives in data folder")

        func = functools.partial(
            _make_dataframe,
            elements=self._get_elements(mode),
            target_codes=self._get_activity_codes(activity_codes, mode),
            debug=True if mode in ("smb",) else False
        )

        for filename in input_files:
            if filename in history:
                print(f"{filename} already processed")
                continue

            path = self._resolve_local_file_path(in_dir, filename)
            print(f"Processing {filename}")
            out_file = pathlib.Path(out_dir) / f"{path.stem}.csv"

            st = time.time()
            archive = Archive(path)

            with multiprocessing.Pool(processes=self._num_workers) as pool:
                for df in tqdm(
                    pool.imap(func, archive, chunksize=self._chunksize),
                    total=len(archive)
                ):
                    if df is None:
                        logging.warning("Empty df returned")
                        continue

                    if out_file.exists():
                        df.to_csv(out_file, index=False, header=False, mode="a")
                    else:
                        df.to_csv(out_file, index=False)

            et = time.time()
            duration = et - st
            print(f"Completed in {duration:.2f}s")

            self._remove_local_file(path)
            history.append(filename)
            self._dump_history(history, history_file_path)
            del archive

    def _download(self, data_path: str, filename: str) -> pathlib.Path:
        print(f"Downloading file from Yandex Disk to {self._temp_dir.name}")

        api_path = "disk/resources/download"
        headers = {
            "Accept": "application/json",
            "Authorization": f"OAuth {self._token}",
            "Content-Type": "application/json",
        }
        params = {
            "path": data_path + "/" + filename,

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

        downloaded_file = pathlib.Path(self._temp_dir.name) / filename
        with open(downloaded_file, "wb") as f:
            for chunk in tqdm(resp.iter_content(2**20)): # chunk size is 1 Mib
                f.write(chunk)

        return downloaded_file

    def _dump_history(self, history: List[str], history_file_path: pathlib.Path):
        with open(history_file_path, "w") as f:
            json.dump(history, f)

    def _get_files(self, directory: str) -> List[str]:
        if self._data_source == "local":
            data_folder = pathlib.Path(directory)
            files = [f.name for f in data_folder.glob("*.zip")]
        else:
            files = self._get_file_list_from_ydisk(directory)

        return files

    def _get_file_list_from_ydisk(self, directory: str) -> List[str]:
        print(f"Getting files list for {directory} on Yandex Disk")

        result = []
        api_path = "disk/resources"
        headers = {
            "Accept": "application/json",
            "Authorization": f"OAuth {self._token}",
            "Content-Type": "application/json",
        }
        params = {
            "path": directory,
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

    def _get_history(self, history_file_path: pathlib.Path) -> List[str]:
        if history_file_path.exists():
            with open(history_file_path) as f:
                history = json.load(f)
        else:
            history = []

        return history

    def _make_out_folder(self, directory: str, clear: bool):
        out_path = pathlib.Path(directory)

        if out_path.exists():
            if clear:
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

    def _resolve_local_file_path(self, data_path: str, filename: str) -> pathlib.Path:
        if self._data_source == "local":
            file_path = pathlib.Path(data_path) / filename
        else:
            file_path = self._download(data_path, filename)

        return file_path

    def _remove_local_file(self, path: pathlib.Path):
        if self._data_source == "local":
            return

        if not local_file.exists():
            return

        local_file.unlink()
        print(f"Local copy of downloaded file at {path} removed")

    def _get_activity_codes(
            self, codes_from_input: List[str], mode: str) -> Optional[List[str]]:
        if mode not in ("smb", ):
            return None

        logging.info("Getting filters by activity code(s)")

        classifier = pd.read_csv(self.ACTIVITY_CODES_CLASSIFIER)
        logging.info(
            f"Found activity codes classifier at {self.ACTIVITY_CODES_CLASSIFIER}"
        )
        codes = []

        for code in codes_from_input:
            code = code.strip()
            if code in string.ascii_uppercase:
                inner_codes = classifier.loc[classifier["group"] == code]
            else:
                inner_codes = classifier.loc[classifier["code"].str.startswith(code)]

            if len(inner_codes) == 0:
                inner_codes = pd.DataFrame(
                    [[np.nan, code, np.nan]],
                    columns=classifier.columns
                )
                logging.warning(f"Code {code} not found in the classifier and will be used as is")

            codes.append(inner_codes)

        if len(codes) == 0:
            logging.info("No filtering by activity codes, using all data")
            codes = None
        else:
            codes = pd.concat(codes)
            codes = codes.loc[codes["code"] != ""]

            logging.info("Activity codes to filter")
            logging.info(codes)

            codes = list(codes["code"])

        return codes

    def _get_elements(self, mode: str):
        if mode not in self.MODES:
            raise RuntimeError(f"(Unknown mode {mode}, expected one of {self.MODES}")

        return elements[mode]


def main():
    extractor = Extractor()
    extractor()


if __name__ == "__main__":
    main()
