import logging
from dotenv import load_dotenv


logger = logging.getLogger(__name__)


def set_env_vars(environment: str="E"):
    """Set environemnts variables
    @param environment: Define corresponding environment
    """
    logger.info(
        f"Setting environment variables for environment: {environment}")
    load_dotenv()

