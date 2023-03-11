import logging

from src.shared_code import utils

logger = logging.getLogger(__name__)


sources = [
    utils.DataAsset(
        name="testing_source",
        kind="local",
        layer="raw",
        path="testing_io/test_reading",
        extension="csv",
        description="features and labels from UCI datasets"
    ),
]


sinks = [
    utils.DataAsset(
        name="testing_sink",
        kind="local",
        layer="raw",
        path="testing_io/test_writting",
        extension="csv",
        description="features and labels from UCI datasets"
    ),
]

