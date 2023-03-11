from abc import abstractmethod, ABC
import shutil
from typing import List, Dict, Callable, Any, Tuple
from enum import Enum

import pandas as pd
import numpy.typing as npt

from src.shared_code.connectors import DataConnectorAbstract
from src.shared_code import utils


class SplitsEnum(Enum):
    TRAIN = "0.train"
    VALID = "1.valid"
    TEST = "2.test"


class DatasetAbstract(ABC):
    """
    Abstract base class defining the interface for a dataset object.

    @param connector: The data connector object to use for this dataset.
    @type connector: DataConnectorAbstract
    """

    def __init__(self, connector: DataConnectorAbstract):
        self.connector = connector

    @abstractmethod
    def read(self,  func, args: Tuple, kwargs: Dict[str, Any]) -> Any:
        """
        Abstract method for reading a file or set of files from the data connector.

        @param func: The function to use for reading the file(s).
        @type func: function
        @param args: Positional arguments to pass to the read function.
        @param kwargs: Keyword arguments to pass to the read function.
        """
        pass

    @abstractmethod
    def write(name: str, func, args: Tuple, kwargs: Dict[str, Any]) -> None:
        """
        Abstract method for writing a Dataset data to a file or set of files to the  data connector.
        @param name: The name of the file to write to.
        @param func: The function to use for writing the file(s).
        @param args: Positional arguments to pass to the write function.
        @param kwargs: Keyword arguments to pass to the write function.
        """
        pass

    @abstractmethod
    def get_subset(self, splits: List[SplitsEnum]):
        """
        Abstract method for getting a data subset by splitting rule.
        @param splits: A list of SplitsEnum values to use for splitting the data.
        @return: DatasetAbstract
        """
        pass


class DatasetPandas(DatasetAbstract):
    """
    Dataset object for working with Pandas DataFrames.

    @param connector: The data connector object to use for this dataset.
    """

    def __init__(self, connector: DataConnectorAbstract):
        self.connector = connector

    def read(self, func: Callable = pd.read_csv, args=(), kwargs={}) -> pd.DataFrame:
        """
        Read a file or set of files from the data connector and return a Pandas DataFrame.
        Also sets source pandas.DataFrame in self.data_

        @param func: The Pandas function to use for reading the file(s). Default is pd.read_csv.
        @param args: Positional arguments to pass to the read function. Default is {}.
        @type args: dict, optional
        @param kwargs: Keyword arguments to pass to the read function. Default is {}.
        @type kwargs: dict, optional
        @return: A Pandas DataFrame containing the data from the file(s).
        """
        io_stream = self.connector.get_data()

        df = func(io_stream, *args, **kwargs)
        self.data_ = df
        self._set_metadata()

        return df

    def write(self, dataframe: pd.DataFrame, args=(), kwargs={}) -> None:
        """
        Write a Pandas DataFrame to the data connector data store

        @param dataframe: The Pandas DataFrame to write to file.
        @param args: Positional arguments to pass to the pandas.DataFrame write function. Default is {}.
        @type args: dict, optional
        @param kwargs: Keyword arguments to pass to the pandas.to_csv() function. Default is {}.
        @type kwargs: dict, optional
        """

        path_temp = utils.path_data / "temp"
        fln_temp = path_temp / f"{self.connector.path}"
        fln_temp.parent.mkdir(parents=True, exist_ok=True)

        dataframe.to_csv(fln_temp, *args, **kwargs)
        self.connector.write_data(fln_temp)
        shutil.rmtree(path_temp)

    def _set_metadata(self):
        """Sets dataset metadata based on the follwing naming conventions:
        features should start with x_
            Those whose type is numeric, are subseted as numeric features,
            whereas those that are object, are subsetd as categorical ones
        targets should start with y_
        id cols should start with id_
        there must be a split col named split
        """
        cols = self.data_
        self.features_ = [c for c in cols if c.startswith("x_")]
        self.targets_ = [c for c in cols if c.startswith("y_")]
        self.ids_ = [c for c in cols if c.startswith("id_")]
        self.splits_ = [c for c in cols if c == "split"]
        self.features_num_ = self.data_[self.features_].select_dtypes(
            'number').columns.tolist()
        self.features_cat_ = self.data_[self.features_].select_dtypes(
            object).columns.tolist()

        self.idx_features_num_ = [
            self.features_.index(c) for c in self.features_num_]
        self.idx_features_cat_ = [
            self.features_.index(c) for c in self.features_cat_]

    def features(self) -> npt.ArrayLike:
        """
        Get the features of the dataset as a NumPy array.

        @return: The features of the dataset.
        """
        return self.data_[self.features_]

    def targets(self) -> npt.ArrayLike:
        """
        Get the targets of the dataset as a NumPy array.

        @return: The targets of the dataset.
        """
        return self.data_[self.targets_]

    def metadata(self) -> Dict[str, List[str]]:
        return {"features": self.features_,
                "target": self.targets_,
                "id_cols": self.ids_,
                "split_col": self.splits_,
                "features_num": self.features_num_,
                "features_cat": self.features_cat_,
                }

    def features_indexes(self) -> Tuple[npt.ArrayLike, npt.ArrayLike]:
        """
        Fetch numeric and categorical features metadata, subseted by their types in source pandas.DataFrame 
        @return: The targets of the dataset.
        """
        return self.idx_features_num_, self.idx_features_cat_

    def get_subset(self, splits: List[SplitsEnum]):
        """
        Get a subset of the source dataset that matches splits values
        Subset source pandas.DataFrame and instanciate a new Dataset
        @return: DatasetPandas
        """
        subsets_labels = [label.value for label in splits]
        df = self.data_.query(f"split in @subsets_labels")

        ds = DatasetPandas(connector=self.connector)
        ds.data_ = df
        ds._set_metadata()

        return ds
