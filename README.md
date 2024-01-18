# Russian small and medium business dataset generator

A tool for creating a tabular georeferenced dataset of all Russian small and medium business companies and individuals from Federal Tax Service (FTS) opendata.

## Quick start

Install Python and dependencies (see the list below), download this repository, unpack it to current folder, open command line and run commands.

### Dataset of all companies with auto-download

`python -m ru_smb_companies --download`

This will download all source data from FTS servers and process it making a huge resulting table with *all* Russian SMB companies. Runtime is large (up to 2 days), disk space required is about 500 Gb.

### Dataset of some companies with auto-download

To reduce disk usage and runtime, you'd better filter by activity code because you probably don't need the whole dataset but instead want to focus on some economic areas.

`python -m ru_smb_companies --download --ac 10.10 --ac D`

This will filter source data leaving only companies with main activity code starting with 10.10 or in group D of the state classifier (OCVED, see `ru_smb_companies/assets/activity_codes_classifier.csv`).

### No auto-download

You can download source zip archives manually and save them in relevant folders:
- from https://www.nalog.gov.ru/opendata/7707329152-rsmp/ to `ru-smb-data/download/smb`;
- from https://www.nalog.gov.ru/opendata/7707329152-revexp/ to `ru-smb-data/download/revexp`;
- from https://www.nalog.gov.ru/opendata/7707329152-sshr2019/ to `ru-smb-data/download/empl`.

After this, run `python -m ru_smb_companies`. This will process downloaded files. Filtering by activity code can be done with `--ac` options, as shown earlier.

### Other options

Run `python -m ru_smb_companies --help` and `python -m ru_smb_companies <subcommand> --help`.

## Dependencies

- typer
- pyspark
- pandas
- numpy
- requests
- beautifulsoup
- tqdm
- fuzzywuzzy
- lxml

