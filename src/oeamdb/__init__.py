"""Documentation about oeamdb."""

import importlib
import logging
import os

__author__ = "William Schueller"
__email__ = "william.schueller@gmail.com"
__version__ = "0.1.0"


_LAZY_IMPORTS = {
    "Oeamdb": "oeamdb",
    "BasgDownloader": "downloaders",
}

from . import sql_adapters

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Only add the StreamHandler if the env var is NOT set to '1'
if os.environ.get("SILENCE_OEAMDB_LOGS","0") != "1":
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)

def __getattr__(name):
    """
    Triggered only when a requested attribute is NOT already in memory.
    """
    if name in _LAZY_IMPORTS:
        submodule_name = _LAZY_IMPORTS[name]

        if submodule_name is None:
            imported_obj = importlib.import_module(name)
            globals()[name] = imported_obj
            return imported_obj
        else:
            module = importlib.import_module(f".{submodule_name}", package=__name__)

            imported_obj = getattr(module, name)

            globals()[name] = imported_obj
            return imported_obj

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    """
    Overriding __dir__ tells the IDE that these lazy classes exist
    """
    return list(globals().keys()) + list(_LAZY_IMPORTS.keys())


__all__ = [
    "Oeamdb",
    "BasgDownloader",
]
