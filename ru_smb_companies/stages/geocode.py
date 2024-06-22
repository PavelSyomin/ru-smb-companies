import pathlib
from typing import Union

from fuzzywuzzy import fuzz, process
import numpy as np
import pandas as pd

from ..assets import get_asset_path
from ..utils.regions import Regions


def _join_name_and_type(n: Union[str, float], t: Union[str, float]) -> str:
    if pd.isna(n) or pd.isna(t):
        return np.nan

    prepend_types = (
        "Город", "Республика", "Поселок", "Поселок городского типа", "Рабочий поселок"
    )
    prepend = t in prepend_types
    if prepend:
        return f"{t} {n}"

    return f"{n} {t}"


def _join_area_and_type(a: Union[str, float], t: Union[str, float]) -> str:
    if pd.isna(a) or pd.isna(t):
        return np.nan

    if t == "г":
        return f"Город {a}"
    elif t == "р-н":
        return f"{a} район"
    elif t == "у":
        return f"{a} улус"
    else:
        return a


def _preprocess_text_column(c: pd.Series) -> pd.Series:
    return c.str.upper().str.replace("Ё", "Е")


class Geocoder:
    ABBR_PATH = get_asset_path("abbr.csv")
    CITIES_BASE_PATH = get_asset_path("cities.csv")
    CITIES_ADDITIONAL_PATH = get_asset_path("cities_additional.csv")
    REGIONS_PATH = get_asset_path("regions.csv")
    SETTLEMENTS_PATH = get_asset_path("settlements.csv")

    ADDR_COLS = [
        "region_name",
        "region_type",
        "district_name",
        "district_type",
        "city_name",
        "city_type",
        "settlement_name",
        "settlement_type",
    ]

    CHUNKSIZE = 1e6

    def __init__(self):
        self._abbr = None
        self._cities = None
        self._regions = None
        self._settlements = None

        self._setup_geodata()

    def __call__(self, in_file: str, out_file: str):
        if not pathlib.Path(in_file).exists():
            print(f"Input file {in_file} not found")
            return

        data = pd.read_csv(in_file, dtype=str, chunksize=self.CHUNKSIZE)

        for i, chunk in enumerate(data):
            print(
                f"Processing chunk #{i}:"
                f" {i * self.CHUNKSIZE:.0f}–{(i + 1) * self.CHUNKSIZE:.0f}"
            )
            chunk["id"] = range(0, chunk.shape[0])

            addresses = self._get_addresses(chunk)
            cities = self._get_cities_standard()
            settlements = self._get_settlements_standard()

            mapping = self._geocode(addresses, cities, settlements)
            chunk = self._remove_raw_addresses(chunk)

            chunk = chunk.merge(mapping, how="left")
            geo = self._get_joint_geodata()

            initial_count = len(chunk)
            chunk = chunk.merge(geo, how="left", on=["geo_id", "type"])
            assert len(chunk) == initial_count
            chunk.drop(columns=["geo_id", "type"], inplace=True)

            initial_count = len(chunk)
            chunk = chunk.merge(
                addresses[["id", "region"]],
                how="left",
                on="id")
            assert len(chunk) == initial_count
            chunk["region"] = chunk["region_x"].combine_first(chunk["region_y"])
            chunk.drop(
                columns=["region_x", "region_y", "region_code"],
                inplace=True
            )

            chunk = self._normalize_region_names(chunk)
            chunk = self._process_federal_cities(chunk)

            chunk = self._remove_duplicates(chunk)

            self._save(chunk, out_file)

    def _get_addresses(self, data: pd.DataFrame) -> pd.DataFrame:
        addresses = data.loc[:, ["id"] + self.ADDR_COLS]
        addresses = self._normalize_address_elements_types(addresses)
        addresses = self._normalize_region_names(addresses)
        addresses.iloc[:, 1:] = addresses.iloc[:, 1:].apply(_preprocess_text_column)

        return addresses

    def _get_cities_standard(self) -> pd.DataFrame:
        std = self._cities[["id", "region", "area", "city", "settlement"]]
        cities_from_areas = self._cities.loc[(self._cities["area_type"] == "г") & (self._cities["city"].isna())].copy()
        cities_from_areas["city"] = cities_from_areas["area"]
        cities_from_areas["area"] = np.nan
        cities_from_areas = cities_from_areas[["id", "region", "area", "city", "settlement"]]
        std = pd.concat((std, cities_from_areas))

        std = self._normalize_region_names(std)
        std.iloc[:, 1:] = std.iloc[:, 1:].apply(_preprocess_text_column)

        return std

    def _get_settlements_standard(self) -> pd.DataFrame:
        std = self._settlements.loc[:, ["id", "region", "municipality", "settlement", "type"]]
        std = self._expand_abbrs(std, "type")
        std = self._normalize_region_names(std)
        std.iloc[:, 1:] = std.iloc[:, 1:].apply(_preprocess_text_column)

        return std

    def _normalize_address_elements_types(
            self, addresses: pd.DataFrame) -> pd.DataFrame:
        for option in ("region", "district", "city", "settlement"):
            target_col = f"{option}_type"
            addresses = self._expand_abbrs(addresses, target_col)

            parts = [f"{option}_name", f"{option}_type"]
            addresses[option] = addresses[parts].apply(
                lambda row: _join_name_and_type(row[parts[0]], row[parts[1]]),
                axis=1
            )

        return addresses

    def _expand_abbrs(self, data: pd.DataFrame, column: str) -> pd.DataFrame:
        initial_count = len(data)
        data[column] = data[column].str.upper()
        data = data.merge(
            self._abbr,
            how="left",
            left_on=column,
            right_on="name",
        )
        data[column] = data["name_full"]
        data.drop(columns=self._abbr.columns, inplace=True)
        assert len(data) == initial_count, (
            f"Number of items must not change, but for {column} "
            f"the size has changed: {initial_count} -> {len(data)}"
        )

        return data

    def _normalize_region_names(self, data: pd.DataFrame) -> pd.DataFrame:
        initial_count = len(data)
        regions = data["region"].dropna().unique()

        regions_norm = []
        regions_codes = []
        regions_iso_codes = []
        for region in regions:
            region_info = self._regions.get(region)
            regions_norm.append(region_info.name)
            regions_codes.append(region_info.code)
            regions_iso_codes.append(region_info.iso_code)

        regions_norm = pd.DataFrame({
            "region": regions,
            "region_norm": regions_norm,
            "region_code": regions_codes,
            "region_iso_code": regions_iso_codes,
        })

        data = data.merge(regions_norm, how="left", on="region")
        data["region"] = data["region_norm"]
        data.drop(columns="region_norm", inplace=True)
        assert len(data) == initial_count

        return data

    def _setup_geodata(self):
        print("Loading geodata")

        self._load_abbr()
        self._load_cities()
        self._load_regions()
        self._load_settlements()

    def _load_abbr(self):
        abbr = pd.read_csv(self.ABBR_PATH)

        short_to_full = abbr[["name", "name_full"]]

        full_to_full = abbr[["name_full", "name_full"]]
        full_to_full.columns = ("name", "name_full")

        without_dots = abbr.loc[~abbr["name"].str.endswith("."), ["name", "name_full"]]
        without_dots["name"] = without_dots["name"] + "."

        abbr_to_full = pd.concat((
            short_to_full,
            full_to_full,
            without_dots
        ))
        abbr_to_full = abbr_to_full.apply(lambda x: x.str.upper())
        abbr_to_full.drop_duplicates("name", inplace=True)

        self._abbr = abbr_to_full
        print("Loaded address abbreviations")

    def _load_cities(self):
        cities_base = pd.read_csv(self.CITIES_BASE_PATH, dtype=str)
        cities_additional = pd.read_csv(self.CITIES_ADDITIONAL_PATH, dtype=str)
        cities = pd.concat((cities_base, cities_additional))
        cities.reset_index(drop=True, inplace=True)
        cities["id"] = range(0, cities.shape[0])
        self._cities = cities
        print("Loaded cities")

    def _load_regions(self):
        self._regions = Regions(self.REGIONS_PATH)
        print("Loaded regions")

    def _load_settlements(self):
        self._settlements = pd.read_csv(self.SETTLEMENTS_PATH, dtype=str)
        print("Loaded settlements")

    def _geocode(
        self,
        addresses: pd.DataFrame,
        cities: pd.DataFrame,
        settlements: pd.DataFrame
    ) -> pd.DataFrame:
        merge_options = [
            {
                "name": "Settlements by all parts with full district name",
                "addresses": ["region", "district", "settlement_name", "settlement_type"],
                "standard": ["region", "municipality", "settlement", "type"],
                "type": "settlements"
            },
            {
                "name": "Settlements by all parts with partial district name (no type)",
                "addresses": ["region", "district_name", "settlement_name", "settlement_type"],
                "standard": ["region", "municipality", "settlement", "type"],
                "type": "settlements",
            },
            {
                "name": "Settlements by all parts with full city name",
                "addresses": ["region", "city", "settlement_name", "settlement_type"],
                "standard": ["region", "municipality", "settlement", "type"],
                "type": "settlements",
            },
            {
                "name": "Settlements by all parts with partial city name (no type)",
                "addresses": ["region", "city_name", "settlement_name", "settlement_type"],
                "standard": ["region", "municipality", "settlement", "type"],
                "type": "settlements",
            },
            {
                "name": "Settlements by all parts except for type with full district name",
                "addresses": ["region", "district", "settlement_name"],
                "standard": ["region", "municipality", "settlement"],
                "type": "settlements",
            },
            {
                "name": "Settlements by all parts except for type with partial district name",
                "addresses": ["region", "district_name", "settlement_name"],
                "standard": ["region", "municipality", "settlement"],
                "type": "settlements",
            },
            {
                "name": "Settlements by all parts except for type with full city name",
                "addresses": ["region", "city", "settlement_name"],
                "standard": ["region", "municipality", "settlement"],
                "type": "settlements",
            },
            {
                "name": "Settlements by all parts except for type with partial city name",
                "addresses": ["region", "city_name", "settlement_name"],
                "standard": ["region", "municipality", "settlement"],
                "type": "settlements",
            },
            {
                "name": "Settlements by region and settlement with type",
                "addresses": ["region", "settlement_name", "settlement_type"],
                "standard": ["region", "settlement", "type"],
                "type": "settlements",
            },
            {
                "name": "Settlements by region and settlement without type",
                "addresses": ["region", "settlement_name"],
                "standard": ["region", "settlement"],
                "type": "settlements",
            },
            {
                "name": "Cities by all parts",
                "addresses": ["region", "district_name", "city_name", "settlement_name"],
                "standard": ["region", "area", "city", "settlement"],
                "type": "cities",
            },
            {
                "name": "Cities by all parts except for settlements",
                "addresses": ["region", "district_name", "city_name"],
                "standard": ["region", "area", "city"],
                "type": "cities",
            },
            {
                "name": "Cities by region and city",
                "addresses": ["region", "city_name"],
                "standard": ["region", "city"],
                "type": "cities",
            },
            {
                "name": "Cities by region and district-as-city",
                "addresses": ["region", "city_name"],
                "standard": ["region", "area"],
                "type": "cities",
            },
        ]

        mappings = []
        rest = addresses
        orig_cols = addresses.columns
        for option in merge_options:
            name = option["name"]
            left_cols = option["addresses"]
            right_cols = option["standard"]
            type_ = option["type"]

            to_merge = rest[orig_cols]
            standard = cities.copy() if type_ == "cities" else settlements.copy()
            standard.drop_duplicates(subset=right_cols, keep=False, inplace=True)
            if len(right_cols) == 2:
                standard.dropna(subset=right_cols, inplace=True)
            standard.rename(columns={"id": "geo_id"}, inplace=True)

            size_before = len(to_merge)
            merged = to_merge.merge(
                standard,
                how="left",
                left_on=left_cols,
                right_on=right_cols,
                suffixes=("", "_x")
            )

            size_after = len(merged)
            assert size_before == size_after

            mapped = merged.loc[merged["geo_id"].notna(), ["id", "geo_id"]]
            mapped["type"] = type_[0]
            if len(mapped) > 0:
                mappings.append(mapped)

            rest = merged.loc[merged["geo_id"].isna()]

            print(f"Option {name}: found {len(mapped)} matches, {len(rest)} records left")

        addr_to_geo = pd.concat(mappings)

        return addr_to_geo

    def _get_joint_geodata(self) -> pd.DataFrame:
        s_cols = [
            "id", "region", "municipality", "settlement", "type",
            "oktmo", "longitude_dd", "latitude_dd"
        ]
        s = self._settlements[s_cols].copy()
        s["geosource_type"] = "s"
        s.rename(columns={
            "id": "geo_id",
            "municipality": "area",
            "type": "settlement_type",
            "longitude_dd": "lon",
            "latitude_dd": "lat",
            "geosource_type": "type",
        }, inplace=True)

        c_cols = [
            "id", "region", "area", "area_type", "city", "city_type",
            "settlement", "settlement_type", "oktmo", "geo_lat", "geo_lon"
        ]
        c = self._cities[c_cols].copy()
        c["settlement"] = (
            c["settlement"]
            .combine_first(c["city"])
            .combine_first(c["area"])
            .reset_index(drop=True)
        )
        c.loc[c["area_type"] == "г", "area"] = np.nan
        c["area"] = c[["area", "area_type"]].apply(
            lambda x: _join_area_and_type(x.iloc[0], x.iloc[1]), axis=1)

        c["geosource_type"] = "c"
        c["settlement_type"] = "г"
        c.rename(columns={
            "id": "geo_id",
            "geo_lat": "lat",
            "geo_lon": "lon",
            "geosource_type": "type",
        }, inplace=True)
        c.drop(columns=["area_type", "city", "city_type"], inplace=True)

        geodata = pd.concat((c, s))

        return geodata

    def _remove_raw_addresses(self, data: pd.DataFrame) -> pd.DataFrame:
        data.drop(columns=self.ADDR_COLS, inplace=True)

        return data

    def _remove_duplicates(self, data: pd.DataFrame) -> pd.DataFrame:
        cols_to_check_for_duplicates = [
            "kind",
            "category",
            "tin",
            "reg_number",
            "first_name",
            "last_name",
            "patronymic",
            "org_name",
            "org_short_name",
            "activity_code_main",
            "region",
            "region_code",
            "region_iso_code",
            "area",
            "settlement",
            "settlement_type",
            "oktmo",
            "lat",
            "lon",
        ]
        duplicates_indices = data.duplicated(
            subset=cols_to_check_for_duplicates,
            keep=False
        )
        duplicates_cleaned = (
            data.loc[duplicates_indices]
            .sort_values("start_date")
            .groupby(cols_to_check_for_duplicates, dropna=False)
            .agg({"id": "first", "start_date": "first", "end_date": "last"})
            .reset_index()
        )

        data = pd.concat((data.loc[~duplicates_indices], duplicates_cleaned))

        return data

    def _process_federal_cities(self, data: pd.DataFrame) -> pd.DataFrame:
        for city in ("Москва", "Санкт-Петербург"):
            data.loc[
                (data["region"] == city) & data["settlement"].isna(),
                "settlement"
            ] = city

        return data

    def _save(self, data: pd.DataFrame, out_file: str):
        product_cols = [
            "tin",
            "reg_number",
            "kind",
            "category",
            "first_name",
            "last_name",
            "patronymic",
            "org_name",
            "org_short_name",
            "activity_code_main",
            "region_iso_code",
            "region_code",
            "region",
            "area",
            "settlement",
            "settlement_type",
            "oktmo",
            "lat",
            "lon",
            "start_date",
            "end_date",
        ]
        product = data[product_cols]

        out_file = pathlib.Path(out_file)
        out_file.parent.mkdir(parents=True, exist_ok=True)

        if out_file.exists():
            product.to_csv(out_file, index=False, header=False, mode="a")
        else:
            product.to_csv(out_file, index=False)
