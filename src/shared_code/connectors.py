from abc import abstractmethod, ABC
from enum import Enum
from pathlib import Path
import logging
import shutil


import os
import io

from azure.storage import blob as azblob
from google.cloud import storage as gcs

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

    Connections clients available in connectionsEnum
        GCS connection needs GCP_PROJECT and GCP_BUCKET as environments variables 

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
        client = ConnectorClientAzBlob(name=connection_name)
    elif connection_kind == connectionsEnum.GCS:
        client = ConnectorClientGCS(
            name=connection_name, project=os.environ["GCP_PROJECT"], bucket=os.environ["GCP_BUCKET"])
    else:
        logger.warning(
            f"Unkwown connection_kind: {connection_kind}, attempting to return a local connection client")
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
    """
    A connector client for interacting with Azure Blob Storage.
    """

    def __init__(self, name: str):
        """
        Initialize an Azure Blob Storage connection client, wrapping a BlobServiceClient
        It is requried that environment variable AZ_STGACC_KEY is set with a storage account key (connection string)
            https://learn.microsoft.com/en-us/python/api/overview/azure/storage-blob-readme?view=azure-python
        @param name: The (descritive) name of the connector client.
        """
        self.name = name
        self._client = azblob.BlobServiceClient.from_connection_string(
            os.environ["AZ_STGACC_KEY"])

    def get_client(self):
        """
        Get the Azure Blob Storage client.

        @return: The Azure Blob Storage client.
        rtype: BlobServiceClient
        """
        return self._client


class ConnectorClientGCS(ConnectorClientAbstract):
    """
    A connector client for interacting with Google Cloud Storage.

    @param name: The name of the connector client.
    @type name: str
    @param project: The name of the GCS project.
    @type project: str
    @param bucket: The name of the GCS bucket.
    @type bucket: str
    """

    def __init__(self, name: str, project: str, bucket: str):
        """
        Initialize the Google Cloud Storage connector client, wrapping a gcs.Client
        It is requried that environment variable GOOGLE_APPLICATION_CREDENTIALS is set with a path to credentials json.
        See also:
            https://googleapis.dev/python/google-api-core/latest/auth.html
            https://cloud.google.com/iam/docs/creating-managing-service-account-keys (Get a service account key)

        @param name: The (descritive) name of GCS connection client
        @param project: The name of the GCP project.
        @param bucket: The name of the GCS bucket.
        """
        self.name = name
        self.project = project
        self.bucket = bucket
        self._client = gcs.Client(project)

    def get_client(self):
        """
        Get the Google Cloud Storage client.

        @return: gcs.Client
        """
        return self._client


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
        @param path: The path to the data source where the data asset is defined (to read or write actual data)
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
    def write_data(self) -> None:
        """
        Write the data to the sink data store
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
        @param path: Complete path with file name and extension, e.g. my-data/2022/01/mydata.csv
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
        """
        path_data = self.connector_client / self.layer / self.path
        path_data.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(filename, path_data)


class DataConnectorAzBlob(DataConnectorAbstract):
    """
    A data connector to fetch data form  Azure Blob Storage
    """

    def __init__(self, connector_client: ConnectorClientAbstract, layer: str, path: str):
        """
        Initialize the Azure Blob Storage data connector, getting the container client instanciated to the proper storage account

        @param connector_client: The connector client for interacting with Azure Blob Storage Account
        @type connector_client: ConnectorClientAzBlob
        @param layer: Container name where the data asset is defined, e.g. raw
        @param path: Complete path in container (not including the layer folder) with file name and extension, e.g. my-data/2022/01/mydata.csv
        """
        self.connector_client = connector_client._client  # blob_serv_client
        self.layer = layer  # container_name
        self.path = path

        # Store metadata
        self.stg_acc_name = self.connector_client.account_name
        self.container_name = self.layer

        # Initialize BlockBlobService, which allows reading bytes
        logger.info(
            f'Setting connection storage account: {self.stg_acc_name} and container: {self.container_name}')

        self.container_client = self.connector_client.get_container_client(
            self.layer)

    def get_data(self):
        """
        Get the data from the Azure Blob Storage container as a bytes stream

        @return: The data as an IO stream.
        """
        logger.info(
            f'Reading file {self.path} from container {self.container_name}')
        print()

        # Read blob
        blob_data = self.container_client.download_blob(self.path)
        # Create IO stream from bytes (it can be read as a file)
        io_stream = io.BytesIO(blob_data.content_as_bytes())
        return io_stream

    def write_data(self) -> None:
        """
        Write data to the Azure Blob Storage container.
        """
        pass


class DataConnectorGCS(DataConnectorAbstract):
    """
    A data connector for interacting with Google Cloud Storage.
    """

    def __init__(self, connector_client: ConnectorClientAbstract, layer: str, path: str):
        """
        Initialize the Google Cloud Storage data connector, getting the bucket client instanciated to the proper storage bucket

        @param connector_client: The connector client for interacting with Google Cloud Storage bucket
        @type connector_client: ConnectorClientGCS
        @param layer: Folder (shoud be in the data/ folder) in bucket where the data asset is defined, e.g. raw
        @param path: Complete path in bucket (not including the layer folder) with file name and extension, e.g. my-data/2022/01/mydata.csv
        """
        self.storage_client = connector_client._client
        self.connector_client = self.storage_client.get_bucket(
            connector_client.bucket)  # bucketClient
        self.layer = layer
        self.path = path

        # Store metadata
        self.project = connector_client.project
        self.bucket = connector_client.bucket

        # Initialize bucket client, which allows reading bytes
        logger.info(
            f"Setting connection to GCP project: {self.project} and GCS bucket: {self.bucket}")

    def get_data(self):
        """
        Get the data from the GCS bucket as a bytes stream

        @return: The data as an IO stream.
        """
        path_data = f"data/{self.layer}/{self.path}"
        logger.info(f'Reading file {path_data} from bucket {self.bucket}')
        print(path_data)

        # Read blob
        blob_data = self.connector_client.blob(path_data)
        # Create IO stream from bytes (it can be read as a file)
        io_stream = io.BytesIO(blob_data.download_as_bytes())

        return io_stream

    def write_data(self) -> None:
        """
        Write data to the GCS bucket.
        """
        pass


def data_connections_factory(data_asset: utils.DataAsset) -> DataConnectorAbstract:
    """
    Factory function to create a new data connection to interact with a given data asset

    @param data_asset: 
    @type: DataAsset
        each data asset has an attribute kind that define the connection client (see connectionsEnum)

    @return: A new data connector based on the data asset metadata
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
        client_azblob = connection_client_factory(
            connection_kind=connectionsEnum.AZBLOB,
            connection_name="conn_azblob")
        connection = DataConnectorAzBlob(
            connector_client=client_azblob,
            layer=data_asset.layer,
            path=f"{data_asset.path}.{data_asset.extension}")

    elif data_asset.kind == connectionsEnum.GCS.value:
        client_gcs = connection_client_factory(
            connection_kind=connectionsEnum.GCS,
            connection_name="conn_gcs")
        connection = DataConnectorGCS(
            connector_client=client_gcs,
            layer=data_asset.layer,
            path=(f"{data_asset.path}.{data_asset.extension}"),

        )

    else:
        logger.warning(
            f"Unknown connection kind: {data_asset.kind}, attempting to return local data connection")
        client_local = connection_client_factory(
            connection_kind=connectionsEnum.LOCAL,
            connection_name="conn_local")
        connection = DataConnectorLocal(
            connector_client=client_local,
            layer=data_asset.layer,
            path=f"{data_asset.path}.{data_asset.extension}")

    return connection
