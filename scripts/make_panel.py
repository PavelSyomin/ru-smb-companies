from typing import Optional

import pandas as pd


class PanelMaker:
    def __call__(self, smb_file: str, out_file: str,
                 revexp_file: Optional[str] = None,
                 empl_file: Optional[str] = None):
        data = pd.read_csv(smb_file, dtype=str)

        data["start_date"] = pd.to_datetime(data["start_date"])
        data["end_date"] = pd.to_datetime(data["end_date"])

        start_year = data["start_date"].dt.year.min()
        end_year = data["end_date"].dt.year.max()
        panel_elems = []
        for year in range(start_year, end_year + 1):
            start_dt = pd.Timestamp(f"{year}-01-01")
            end_dt = pd.Timestamp(f"{year}-12-31")
            yearly_data = data.sort_values("start_date").loc[~((data["start_date"] > end_dt) | (data["end_date"] < start_dt))].copy()
            group_sizes = yearly_data.groupby("tin", as_index=False).size()
            yearly_data.drop_duplicates(subset=["tin"], keep="last", inplace=True)
            yearly_data["year"] = str(year)
            yearly_data = yearly_data.merge(group_sizes, how="left", on="tin")
            yearly_data["confidence"] = 1 / yearly_data["size"]
            yearly_data.drop(columns=["start_date", "end_date", "size"], inplace=True)
            panel_elems.append(yearly_data)
        panel = pd.concat(panel_elems)

        if revexp_file is not None:
            revexp = pd.read_csv(revexp_file, dtype=str)
            panel = panel.merge(revexp, how="left", on=["tin", "year"])

        if empl_file is not None:
            empl = pd.read_csv(empl_file, dtype=str)
            panel = panel.merge(empl, how="left", on=["tin", "year"])

        panel.to_csv(out_file, index=False)
