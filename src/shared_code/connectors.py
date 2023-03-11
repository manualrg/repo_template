from abc import abstractmethod, ABC
from enum import Enum
from pathlib import Path
import logging
import shutil

from src.shared_code import utils
logger = logging.getLogger(__name__)


class connectionsEnum(Enum):
    """
    Enumeration class that defines valid connection kinds.

    Valid values are LOCAL, AZBLOB, and GCS.
    """
    LOCAL = "local"
    AZBLOB = "azblob"
    GCS = "gcs"


def connection_client_factory(connection_kind: connectionsEnum, connection_name: str = "source"):
    """
    Factory function that returns an instance of a ConnectorClient object for the given connection kind.

    @param connection_kind: The kind of connection to create. Should be a member of connectionsEnum.
    @type connection_kind: connectionsEnum

    @param connection_name: The name of the connection to create. Default is 'source'.
    @type connection_name: str, optional

    @return: An instance of a ConnectorClient object for the given connection kind.
    :rtype: ConnectorClient
    """
    if connection_kind == connectionsEnum.LOCAL:
        client = ConnectorClientLocal(name=connection_name)
    elif connection_kind == connectionsEnum.AZBLOB:
        logger.warning(
            f"connection_kind: {connection_kind}. not implemented")
        pass
    elif connection_kind == connectionsEnum.GCS:
        logger.warning(
            f"connection_kind: {connection_kind}. not implemented")
        pass
    else:
        logger.warning(
            f"Unkwown connection_kind: {connection_kind}. Defaulting to")
        client = ConnectorClientLocal(name=connection_name)

    return client


class ConnectorClientAbstract(ABC):
    """
    Abstract base class for a connection client.

    This class defines an interface for connecting to different kinds of data sources.
    """

    def __init__(self, name: str = "connection"):
        """
        Initialize the ConnectorClient object.

        @param name: The name of the connection. Default is 'connection'.
        @type name: str, optional
        """
        self.name = name

    @abstractmethod
    def get_client(self):
        """
        Get the connection client for this ConnectorClient.

        This method should be implemented in subclasses.
        """
        pass


class ConnectorClientLocal(ConnectorClientAbstract):
    """
    Connection client for a local data source.

    This client reads data from a local directory specified by the path_data variable in the utils module.
    """

    def __init__(self, name: str):
        """
        Initialize the ConnectorClientLocal object.
        The connection client of a local connection is a Path object pointing to local data path

        @param name: The name of the connection. This parameter is not used in this implementation.
        @type name: str
        """
        self.name = name
        self._client = utils.path_data

    def get_client(self) -> Path:
        """
        Get the connection client for this ConnectorClientLocal.

        @return: The path to the local data directory.
        :rtype: Path
        """
        return self._client


class ConnectorClientAzBlob(ConnectorClientAbstract):

    def __init__(self, name: str):
        self.name = name
        self._client = None  # AzBlobClient

    def get_client(self):
        pass


class ConnectorClientGCS(ConnectorClientAbstract):

    def __init__(self, name: str):
        self.name = name
        self._client = None  # GCSClient

    def get_client(self):
        pass


class DataConnectorAbstract(ABC):
    """
    Abstract base class for a data connector.

    This class defines an interface for reading/writting data from different kinds of data storages
    """

    def __init__(self, connector_client: ConnectorClientAbstract, layer: str, path: str):
        """
        Initialize the DataConnectorAbstract object.
        @param connector_client: The connection client required to stablish the data connection
        @param layer: Data layer where path is defined
        @param path: The path to the data source where the data asset is stored
        """
        self.connector_client = connector_client
        self.layer = layer
        self.path = path

    @abstractmethod
    def get_data(self):
        """
        Read the data from the data source and return it.

        This method should be implemented in subclasses.
        """
        pass

    @abstractmethod
    def write_data(self, filename: Path) -> None:
        """
        Write the data to the sink data store
        @param filename: The file path of the temporary file containing the data to be written.
        """
        pass


class DataConnectorLocal(DataConnectorAbstract):
    """
    A concrete implementation of DataConnectorAbstract that interacts with a local data store.
    """

    def __init__(self, connector_client: ConnectorClientAbstract, layer: str, path: str):
        """
        Initialize a new instance of DataConnectorLocal. This will allow to read/write data assets to a local data store

        @param connector_client: An connectorclient instance to establish a connection with the data store.
        @type connector_client: ConnectorClientLocal
        @param layer: Subfolder in data/ where the data asset is defined, e.g. raw
        @type layer: str
        @param path: Complete path with file name and extension, e.g. my-data/2022/01/mydata.csv
        @type path: str
        """
        self.connector_client = connector_client._client
        self.layer = layer
        self.path = path

    def get_data(self):
        """
        Read the data from the local data source and return it.
        @return: The data as a memory object from the local data store (source).
        :rtype: file object
        """
        path_data = self.connector_client / self.layer / self.path
        return open(path_data, "r")

    def write_data(self, filename: Path) -> None:
        """
        Write the data to the local data store (sink)

        @param filename: The file path of the temporary file containing the data to be written.
        @type filename: str
        """
        path_data = self.connector_client / self.layer / self.path
        path_data.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(filename, path_data)


class DataConnectorAzBlob(DataConnectorAbstract):

    def __init__(self, connector_client: ConnectorClientAbstract, layer: str, path: str):
        self.connector_client = connector_client._client
        self.layer = layer
        self.path = path

    def get_data(self):
       pass

    def write_data(self, filename: Path) -> None:
       pass


class DataConnectorGCS(DataConnectorAbstract):

    def __init__(self, connector_client: ConnectorClientAbstract, layer: str, path: str):
        self.connector_client = connector_client._client
        self.layer = layer
        self.path = path

    def get_data(self):
        pass

    def write_data(self, filename: Path) -> None:
        pass


def data_connections_factory(data_asset: utils.DataAsset) -> DataConnectorAbstract:
    """
    Factory function to create a new data connection to interact with stored data stored given a data asset definition

    @param source: The source data connection.
    @return: A new data connector based on the source data connection.
    """
    logger.info(
        f"Setting connection to data asset: {data_asset.name} of kind: {data_asset.kind} to layer: {data_asset.layer} and path: {data_asset.path}")

    if data_asset.kind == connectionsEnum.LOCAL.value:
        client_local = connection_client_factory(
            connection_kind=connectionsEnum.LOCAL,
            connection_name="conn_local")
        connection = DataConnectorLocal(
            connector_client=client_local,
            layer=data_asset.layer,
            path=f"{data_asset.path}.{data_asset.extension}")

    elif data_asset.kind == connectionsEnum.AZBLOB.value:
        logger.warning(
            f"connection: {data_asset.kind}. not implemented")
        pass

    elif data_asset.kind == connectionsEnum.GCS.value:
        logger.warning(
            f"connection: {data_asset.kind}. not implemented")
        pass

    else:
        logger.warning(f"unknown connection, returning local")
        client_local = connection_client_factory(
            connection_kind=connectionsEnum.LOCAL,
            connection_name="conn_local")
        connection = DataConnectorLocal(
            connector_client=client_local,
            layer=data_asset.layer,
            path=f"{data_asset.path}.{data_asset.extension}")

    return connection
