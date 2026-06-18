"""Documentation about oeamdb."""

import importlib
import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())

__author__ = "William Schueller"
__email__ = "william.schueller@gmail.com"
__version__ = "0.1.0"


_LAZY_IMPORTS = {
	"Oeamdb":"oeamdb",
	"BasgDownloader":"downloaders",
}


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
