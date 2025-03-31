import pathlib

from make_csv import Config, XML2CSVExtractor


class ValDataExtractor(XML2CSVExtractor):
    FILES = [
        "data-12122016-structure-08012016.zip",
        "data-12112017-structure-08012016.zip",
        "data-12102018-structure-08012016.zip",
        "data-12102019-structure-10102019.zip",
        "data-10122020-structure-15052020.zip",
        "data-10122021-structure-10082021.zip",
        "data-10122022-structure-10032022.zip",
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._debug = False
        self._target_codes = None

    def _get_elements(self):
        return {
            "@ДатаСост": "data_date",
            "ИПВклМСП/@ИННФЛ": "ind_tin",
            "ОргВклМСП/@ИННЮЛ": "org_tin",
            "СведМН/Регион/@Наим": "region_name",
            "СвОКВЭД/СвОКВЭДОсн/@КодОКВЭД": "activity_code_main",
        }

    def _get_files(self):
        return self.FILES


def main():
    config = Config(
        "rsmp/xml", "reestr", "rsmp/reestr_test", "local", True, 3, 32, [], None)
    extractor = ValDataExtractor(config)
    extractor.run()


if __name__ == "__main__":
    main()
