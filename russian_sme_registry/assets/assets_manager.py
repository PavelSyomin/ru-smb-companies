import pathlib


def get_asset_path(name: str) -> str:
    path = pathlib.Path(__file__).parent / name

    return str(path)
