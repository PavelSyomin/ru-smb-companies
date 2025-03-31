# Russian SME registry dataset generator

A tool for creating a georeferenced dataset of all Russian small and medium-sized enterprises (SME) from Federal Tax Service (FTS) opendata.

## Installation

`pip install russian-sme-registry`

## Usage 

The extensive documentation is available via `--help` command (`russian-sme-registry --help`), so consult it first. Here is just a bfier description of a few use cases.

### Make dataset of *all* SMEs with auto-download

`russian-sme-registry process --download`

This command will try to download *all* source data from FTS servers and process it making a huge resulting CSV table with *all* Russian small and medium-sized enterprises. Note that source data is approximately 200 Gb in size, and intermediary and resulting files take about the same amount of disk space, thus ≈500 Gb of free disk space is recommended. The time to process *all* the data is also relatively high: up to several days depending on the CPU capability.

### Make dataset of *some* SMEs with auto-download

`russian-sme-registry process --download --ac 10.10 --ac D`

You can filter SMEs by NACE Rev.2-compatible activity code («ОКВЭД» in Russia) with `--ac` option. This filtration can drastically reduce the size of intermediary and output files and desrease the processing time. However, the volume of downloaded source files is still large.

You can filter either by NACE group (e. g. `--ac D`) or by a particular code (e. g. `--ac 10.10`). Note that group or code include all downstream codes, e. g. `--ac 10.10` selects SMEs with activity codes `10.10`, `10.10.1`, `10.10.2`, `10.10.1.1` (if present in the classifier). NACE classifier can be found online. Its Russian equivalent (ОКВЭД) is stored in `russian_sme_registry/assets/activity_codes_classifier.csv` and is internally used by the app.

### Make dataset from already downloaded files

`russian-sme-registry process`

This command first looks at the content of `russian-sme-registry-data` folder in the current working directory and tries to detect source data files inside. If there are source files, it processes them in the same way as auto-download command but without downloading the files. This is useful if you have already downloaded all source data or if you need only a part of source data (e. g. for a given year or just one file per year).

There are three FTS opendata resources:

- [Registry of small and medium-sized enterprises](https://www.nalog.gov.ru/opendata/7707329152-rsmp/) is the main dataset. It is expected that all its source files are downloaded to `russian-sme-registry-data/download/sme`;
- [Data on revenue and expenditure of organizations](https://www.nalog.gov.ru/opendata/7707329152-revexp/) is an additional dataset that enriches SMEs with information about revenue and expenditure of ogranisations (no sole traders data);
- [Data on number of employees of organisations](https://www.nalog.gov.ru/opendata/7707329152-sshr2019/) is another additional dataset. It enriches SMEs with information about number of employees of organisations (again, no sole traders data).

Registry is mandatory, while revenue/expenditure and employees data is optional.

Source data is expected to be stored in the following folders:

- `russian-sme-registry-data/download/smb` for registry;
- `russian-sme-registry-data/download/revexp` for revenue/expenditure;
- `russian-sme-registry-data/download/empl` for employees.

It is also possible to filter with `--ac` option here.

### Running separate stages

Overall, the app workflow consists of five consequtive stages: `download`, `extract`, `aggregate`, `geocode` and `panelize`. They are implemented as separate classes, and each has its own subcommand. You can run `russian-sme-registry --help` for the list of subcommands and `russian-sme-registry <subcommand> --help` for a help with a particular subcommand. In a simple case, you propably will not need to run subcommands, but they may become useful in more complex scenarios. Note that `process` is just a wrapper for `extract`, `aggregate`, `geocode` and `panelize` subcommands, and `process --download` is a shortcut for all the five subcommands.

### Local *vs* cloud

The tool can store source data either locally or using Yandex Disk. If you want to use Yandex.Disk, you should set `storage` and `token` options with `russian-sme-registry config --storage ydisk --token <token>` command. Token can be optained using the [Yandex Disk API instruction](https://yandex.ru/dev/disk-api/doc/ru/concepts/quickstart#oauth). Note that `ydisk` storage support is not tested as well as local mode, thus there may be unexpected bugs. It works for now, but I am not sure whether I am going to maintain it e. g. when Yandex Disk API change.

### Minor options

You can set number of workers (`russian-sme-registry config --num-workers`) and chunksize (`russian-sme-registry config --chunksize`). These options are used by the extract stage only. Increasing number of workers can boost the performance on multi-core CPUs. It is recommended to set it equal to the number of CPU cores or to the number of CPU cores minus 1 (if you want to continue using your machine while program run).

## Output dataset structure

The table below describes the structure of resulting panel CSV file generated by panelize stage. If `process` command is used, the panel file should be located at `russian-sme-registry/panelize` folder.

| Table field | Description | Data type |
|-------------|-------------|-----------|
| tin | Taxpayer Identification Number (TIN) | String |
| year | Year for which the information is given. It may be repeated for each TIN if the data on the SME subject changed during the year. | Integer |
| reg_number | Primary State Registration Number (OGRN) | String |
| kind | Type of SME: 1 — juridical person, 2 — sole trader, 3 — head of peasant (farm) enterprise | Integer |
| category | Category of SME: 1 - micro enterprise, 2 - small enterprise, 3 - medium enterprise | Integer |
| org_name | Full name of the juridical person | String |
| org_short_name | Short name of the juridical person | String |
| activity_code_main | Main activity code compatible with [NACE Rev. 2](https://ec.europa.eu/eurostat/web/products-manuals-and-guidelines/-/ks-ra-07-015) | String |
| region_iso_code | Code of the federal subject where the enterprise is registered according to [ISO 3166](https://www.iso.org/obp/ui/#iso:code:3166:RU) | String |
| region_code | Code of the constituent entity of the federation where the enterprise is incorporated according to the [classification of the Federal Tax Service of Russia](https://esnsi.gosuslugi.ru/classifiers/5709/data?pg=1&p=1) | String |
| region | Name of the constituent entity of the federation where the enterprise is incorporated | String |
| area | Part of the territory of the region (district, municipality) where the enterprise is incorporated | String |
| settlement | Settlement where the enterprise is incorporated | String |
| settlement_type | Abbreviated type of the settlement where the enterprise is incorporated. Abbreviations according to Federal Information Address System (FIAS) | String |
| oktmo | Code of the municipality where the enterprise is incorporated. | String |
| lat | Geographical latitude of the settlement where the enterprise is incorporated (EPSG:4326) | Float |
| lon | Geographic longitude of the settlement where the enterprise is incorporated (EPSG:4326) | Float |
| start_date | Start date of the period during which the information is actual. The period may cover several years — in this case the rows for each year are duplicated. Date format is `yyyy-mm-dd` | String |
| end_date | End date of the period during which the information is relevant. The period may cover several years — in this case the lines for each year are duplicated. Date format is `yyyy-mm-dd` | String |
| revenue | Enterprise's income for the year (for ogranisations only) | Float |
| expenditure | Enterprise's expenses for the year (for ogranisations only) | Float |
| employees_count | Average number of employees of the enterprise for the given year (for ogranisations only) | Integer |

## Known data limitations

1. The belonging to a particular region, district, settlement, as well as the particular municipal code is as of 2021 regardless of the year for which the data is generated. Tracking changes in the territorial structure of Russia is a complex task, thus the geocoding algorithm of this tool uses a fixed-date lookup tables that were released at the end of 2021 and at the beginning of 2022.
2. Revenue and expenditure data from FTS is known to be incomplete and sometimes incorrect (e.g. 200B revenue for a no-name juridical person with 3 employees).
3. Average list employees count is quite a complex measure with a peculiar method of calculation. It does not always effectively represent the company size because omits e. g. contract workers.
4. SMEs may be indirectly controlled by a large ogranisation but still present in the registry.
5. Address of incorporation does not necessarily match the address of doing business.
6. Though SME registry is updated autometically without any action required on the enterprise's part, it still can be incomplete with some SMEs missing.

## Testing

The software includes automated tests (see `tests` folder).

## Contributing and Development

1. If you find a bug or have a feature request, please create an issue.
2. If you want to join the development of the app, please contact me.
