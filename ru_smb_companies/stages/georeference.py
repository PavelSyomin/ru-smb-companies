import pathlib
from typing import Union

from fuzzywuzzy import fuzz, process
import numpy as np
import pandas as pd

from ..assets import get_asset_path
from ..utils.regions import Regions


def join_name_and_type(name, type_):
    if pd.isna(name) or pd.isna(type_):
        return np.nan

    prepend_types = (
        "Город", "Республика", "Поселок", "Поселок городского типа", "Рабочий поселок"
    )
    prepend = type_ in prepend_types
    if prepend:
        return f"{type_} {name}"

    return f"{name} {type_}"


def join_area_and_type(a, t):
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


def preprocess_text_column(c):
    return c.str.upper().str.replace("Ё", "Е")


class Georeferencer:
    ABBR_PATH = get_asset_path("socrbase.csv")
    CITIES_BASE_PATH = get_asset_path("cities.csv")
    CITIES_ADDITIONAL_PATH = get_asset_path("cities_additional.csv")
    REGIONS_PATH = get_asset_path("regions.csv")
    SETTLEMENTS_PATH = get_asset_path("settlements.csv")

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

        data = pd.read_csv(in_file, dtype=str)
        data["id"] = range(0, data.shape[0])

        addresses = self._get_addresses(data)
        cities = self._get_cities_standard()
        settlements = self._get_settlements_standard()

        mapping = self._georeference(addresses, cities, settlements)

        data = data.merge(mapping, how="left")
        geodata = self._get_geodata()

        initial_count = len(data)
        data = data.merge(geodata, how="left", on=["geo_id", "type"])
        assert len(data) == initial_count

        data = self._finalize(data, addresses)
        data = self._remove_duplicates(data)

        self._save(data, out_file)

    def _get_addresses(self, data: pd.DataFrame) -> pd.DataFrame:
        address_cols = [
            "region_name",
            "region_type",
            "district_name",
            "district_type",
            "city_name",
            "city_type",
            "settlement_name",
            "settlement_type",
        ]
        addresses = data.loc[:, ["id"] + address_cols]
        addresses = self._normalize_address_elements_types(addresses)
        addresses = self._normalize_regions_names(addresses)
        addresses.iloc[:, 1:] = addresses.iloc[:, 1:].apply(preprocess_text_column)

        return addresses

    def _get_cities_standard(self):
        std_c = self._cities[["id", "region", "area", "city", "settlement"]]
        cities_from_areas = self._cities.loc[(self._cities["area_type"] == "г") & (self._cities["city"].isna())].copy()
        cities_from_areas["city"] = cities_from_areas["area"]
        cities_from_areas["area"] = np.nan
        cities_from_areas = cities_from_areas[["id", "region", "area", "city", "settlement"]]
        std_c = pd.concat((std_c, cities_from_areas))

        std_c.iloc[:, 1:] = std_c.iloc[:, 1:].apply(preprocess_text_column)

        return std_c

    def _get_settlements_standard(self):
        std_s = self._settlements.loc[:, ["id", "region", "municipality", "settlement", "type"]]
        std_s["type"] = std_s["type"].str.upper()
        std_s = std_s.merge(
            self._abbr,
            how="left",
            left_on="type",
            right_on="name"
        )
        std_s["type"] = std_s["name_full"]
        std_s.drop(columns=self._abbr.columns, inplace=True)
        std_s.iloc[:, 1:] = std_s.iloc[:, 1:].apply(preprocess_text_column)

        return std_s

    def _normalize_address_elements_types(self, addresses):
        initial_count = len(addresses)

        for option in ("region", "district", "city", "settlement"):
            target_col = f"{option}_type"
            addresses = addresses.merge(
                self._abbr,
                how="left",
                left_on=target_col,
                right_on="name",
            )
            addresses[target_col] = addresses["name_full"]
            addresses.drop(columns=self._abbr.columns, inplace=True)
            assert len(addresses) == initial_count, (
                f"Number of addresses must not change, but for {target_col} "
                f"the size has changed: {initial_count} -> {len(addresses)}"
            )

            parts = [f"{option}_name", f"{option}_type"]
            addresses[option] = addresses[parts].apply(
                lambda row: join_name_and_type(row[parts[0]], row[parts[1]]),
                axis=1
            )

        return addresses

    def _normalize_regions_names(self, addresses):
        initial_count = len(addresses)
        unique_region_names = addresses["region_name"].dropna().unique()
        unique_regions_in_cities = self._cities["region"].dropna().unique()
        region_names_to_cities = pd.DataFrame({
            "region_name": unique_region_names,
            "region_cities": [
                self._regions.get(region_name).short_name
                for region_name in unique_region_names
            ]
        })

        unique_regions = addresses["region"].dropna().unique()
        unique_regions_in_settlements = self._settlements["region"].dropna().unique()
        regions_to_settlements = pd.DataFrame({
            "region": unique_regions,
            "region_settlements": [
                self._regions.get(region_name).name
                for region_name in unique_regions
            ]
        })

        addresses = addresses.merge(region_names_to_cities, how="left", on="region_name")
        addresses = addresses.merge(regions_to_settlements, how="left", on="region")
        assert len(addresses) == initial_count

        return addresses

    def _setup_geodata(self):
        print("Loading geodata")

        self._load_abbr()
        self._load_cities()
        self._load_regions()
        self._load_settlements()

    def _load_abbr(self):
        abbr = pd.read_csv(self.ABBR_PATH)

        full_to_full = abbr[["name_full", "name_full"]]
        full_to_full.columns = ("name", "name_full")

        without_dots = abbr.loc[~abbr["name"].str.endswith("."), ["name", "name_full"]]
        without_dots["name"] = without_dots["name"] + "."

        abbr_to_full = pd.concat((
            abbr[["name", "name_full"]],
            full_to_full,
            without_dots
        ))
        abbr_to_full["name"] = abbr_to_full["name"].str.upper()
        abbr_to_full.drop_duplicates("name", inplace=True)

        self._abbr = abbr_to_full
        print("Loaded address abbreviations")

    def _load_cities(self):
        cities_base = pd.read_csv(self.CITIES_BASE_PATH)
        cities_additional = pd.read_csv(self.CITIES_ADDITIONAL_PATH)
        cities = pd.concat((cities_base, cities_additional))
        cities.reset_index(drop=True)
        cities["id"] = range(0, cities.shape[0])
        self._cities = cities
        print("Loaded cities")

    def _load_regions(self):
        self._regions = Regions(self.REGIONS_PATH)
        print("Loaded regions")

    def _load_settlements(self):
        self._settlements = pd.read_csv(self.SETTLEMENTS_PATH)
        print("Loaded settlements")

    def _georeference(self, addresses, cities, settlements):
        merge_options = [
            {
                "name": "Settlements by all parts with full district name",
                "addresses": ["region_settlements", "district", "settlement_name", "settlement_type"],
                "standard": ["region", "municipality", "settlement", "type"],
                "type": "settlements"
            },
            {
                "name": "Settlements by all parts with partial district name (no type)",
                "addresses": ["region_settlements", "district_name", "settlement_name", "settlement_type"],
                "standard": ["region", "municipality", "settlement", "type"],
                "type": "settlements",
            },
            {
                "name": "Settlements by all parts with full city name",
                "addresses": ["region_settlements", "city", "settlement_name", "settlement_type"],
                "standard": ["region", "municipality", "settlement", "type"],
                "type": "settlements",
            },
            {
                "name": "Settlements by all parts with partial city name (no type)",
                "addresses": ["region_settlements", "city_name", "settlement_name", "settlement_type"],
                "standard": ["region", "municipality", "settlement", "type"],
                "type": "settlements",
            },
            {
                "name": "Settlements by all parts except for type with full district name",
                "addresses": ["region_settlements", "district", "settlement_name"],
                "standard": ["region", "municipality", "settlement"],
                "type": "settlements",
            },
            {
                "name": "Settlements by all parts except for type with partial district name",
                "addresses": ["region_settlements", "district_name", "settlement_name"],
                "standard": ["region", "municipality", "settlement"],
                "type": "settlements",
            },
            {
                "name": "Settlements by all parts except for type with full city name",
                "addresses": ["region_settlements", "city", "settlement_name"],
                "standard": ["region", "municipality", "settlement"],
                "type": "settlements",
            },
            {
                "name": "Settlements by all parts except for type with partial city name",
                "addresses": ["region_settlements", "city_name", "settlement_name"],
                "standard": ["region", "municipality", "settlement"],
                "type": "settlements",
            },
            {
                "name": "Settlements by region and settlement with type",
                "addresses": ["region_settlements", "settlement_name", "settlement_type"],
                "standard": ["region", "settlement", "type"],
                "type": "settlements",
            },
            {
                "name": "Settlements by region and settlement without type",
                "addresses": ["region_settlements", "settlement_name"],
                "standard": ["region", "settlement"],
                "type": "settlements",
            },
            {
                "name": "Cities by all parts",
                "addresses": ["region_cities", "district_name", "city_name", "settlement_name"],
                "standard": ["region", "area", "city", "settlement"],
                "type": "cities",
            },
            {
                "name": "Cities by all parts except for settlements",
                "addresses": ["region_cities", "district_name", "city_name"],
                "standard": ["region", "area", "city"],
                "type": "cities",
            },
            {
                "name": "Cities by region and city",
                "addresses": ["region_cities", "city_name"],
                "standard": ["region", "city"],
                "type": "cities",
            },
            {
                "name": "Cities by region and district-as-city",
                "addresses": ["region_cities", "city_name"],
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

    def _get_geodata(self):
        geodata_s = self._settlements[["id", "region", "municipality", "settlement", "type", "oktmo", "longitude_dd", "latitude_dd"]].copy()
        geodata_s["geosource_type"] = "s"
        geodata_s.rename(columns={
            "id": "geo_id",
            "municipality": "area",
            "type": "settlement_type",
            "longitude_dd": "lon",
            "latitude_dd": "lat",
            "geosource_type": "type",
        }, inplace=True)
        geodata_s["region"] = geodata_s["region"].apply(lambda x: self._regions.get(x).name)
        geodata_s.head()

        geodata_c = self._cities[["id", "region", "region_type", "area", "area_type", "city", "city_type", "settlement", "settlement_type", "oktmo", "geo_lat", "geo_lon"]].copy()
        geodata_c["settlement"] = self._cities["settlement"].combine_first(self._cities["city"]).combine_first(self._cities["area"]).reset_index(drop=True)
        geodata_c.loc[geodata_c["area_type"] == "г", "area"] = np.nan
        geodata_c["area"] = geodata_c[["area", "area_type"]].apply(lambda x: join_area_and_type(x[0], x[1]), axis=1)

        regions_c_s = pd.DataFrame({
            "region": geodata_c["region"].unique(),
            "region_norm": [
                self._regions.get(r).name
                for r in geodata_c["region"].unique()]
        })

        geodata_c = geodata_c.merge(regions_c_s, how="left")
        geodata_c["region"] = geodata_c["region_norm"]
        geodata_c["geosource_type"] = "c"
        geodata_c["settlement_type"] = "г"
        geodata_c.rename(columns={
            "id": "geo_id",
            "geo_lat": "lat",
            "geo_lon": "lon",
            "geosource_type": "type",
        }, inplace=True)
        geodata_c.drop(columns=["region_type", "area_type", "city", "city_type", "region_norm"], inplace=True)
        geodata_c.head(3)

        geodata = pd.concat((geodata_c, geodata_s))

        return geodata

    def _finalize(self, data, addresses):
        initial_count = len(addresses)
        addresses["address_raw"] = addresses.loc[:, "region_name":"settlement_type"].apply(lambda x: " / ".join(x.fillna("").to_list()), axis=1)

        unique_regions = addresses["region"].dropna().unique()
        unique_regions_in_settlements = self._settlements["region"].dropna().unique()
        regions_to_settlements = pd.DataFrame({
            "region": unique_regions,
            "region_settlements": [
                self._regions.get(region_name).name
                for region_name in unique_regions
            ]
        })
        regions_to_settlements["region"] = regions_to_settlements["region"].str.upper()
        regions_to_settlements.rename(columns={"region_settlements": "region_norm"}, inplace=True)
        addresses = addresses.merge(
            regions_to_settlements,
            how="left",
        )
        assert len(addresses) == initial_count

        data = data.merge(
            addresses[["id", "address_raw", "region_norm"]],
            how="left",
            on="id",
        )
        assert len(data) == initial_count
        data.drop(
            columns=[
                "region_name", "region_type", "district_name", "district_type",
                "city_name", "city_type", "settlement_name", "geo_id",
                "type", "settlement_type_x"],
            inplace=True
        )

        data["region"] = data["region"].combine_first(data["region_norm"])

        data.rename(columns={"settlement_type_y": "settlement_type"}, inplace=True)

        return data

    def _remove_duplicates(self, data):
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
            .agg({"id": "first", "address_raw": "first", "start_date": "first", "end_date": "last"})
            .reset_index()
        )
        duplicates_cleaned.head(3)

        data = pd.concat((data.loc[~duplicates_indices], duplicates_cleaned))

        return data

    def _save(self, data, out_file):
        product_cols = [
            "id",
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
            "region",
            "area",
            "settlement",
            "settlement_type",
            "oktmo",
            "lat",
            "lon",
            "address_raw",
            "start_date",
            "end_date",
        ]
        product = data[product_cols]

        pathlib.Path(out_file).parent.mkdir(parents=True, exist_ok=True)
        product.to_csv(out_file, index=False)
