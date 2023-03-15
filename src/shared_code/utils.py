from pathlib import Path
import logging
from logging import config
import yaml
from dataclasses import dataclass


path_root = Path(__file__).parent.parent.parent

path_data = path_root / "data"
path_data_raw = path_data / "raw"
path_data_interim = path_data / "interim"
path_data_processed = path_data / "processed"

data_paths = {
    "raw": path_data_raw,
    "interim": path_data_interim,
    "processed": path_data_processed
}


path_conf = path_root / "conf"
path_models = path_root / "models"


def get_conf(path: Path = path_conf, filename: str = 'conf.yaml'):
    """ Returns the configuration as a dictionary object loaded from the YAML file located
    at the given path and with the specified filename.

    @param path: A path-like object representing the directory path where the YAML file is located. 
                 Default is the value of `path_conf`.
    @type path: Path, optional
    @param filename: A string representing the filename of the YAML file to be loaded. 
                     Default is 'conf.yaml'.
    @type filename: str, optional
    @return: A dictionary object containing the configuration values.
    :rtype: dict"""
    fln_conf = path / filename

    with open(fln_conf, "r") as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.BaseLoader)

    return cfg


@dataclass
class DataAsset:
    """
    Metadata associated to a given data asset
    """
    name: str           # descriptive name
    kind: str           # from connections.connectionsEnum
    layer: str          # specific layer, see utils.data_paths
    path: str           # data assset path. Follow naming convention
    extension: str      # without .
    description: str    # description of the data asset


data_asset_dataset = DataAsset(
    name="dataset",
    kind="KIND",
    layer="raw",
    path="YYYY/MM/dataset/",
    extension="csv",
    description="features and labels dataset"
)


data_asset_train = DataAsset(
    name="train",
    kind="KIND",
    layer="interim",
    path="YYYY/MM/train/",
    extension="csv",
    description="training dataset"
)

data_asset_features = DataAsset(
    name="features",
    kind="KIND",
    layer="interim",
    path="YYYY/MM/features/",
    extension="csv",
    description="features dataset"
)

data_asset_preds = DataAsset(
    name="preds",
    kind="KIND",
    layer="processed",
    path="YYYY/MM/preds/",
    extension="csv",
    description="predictions dataset"
)
