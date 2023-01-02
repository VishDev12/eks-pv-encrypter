import logging
from rich.logging import RichHandler


logger = logging.getLogger("ebs_pv_encrypter")

rich_handler = RichHandler(
    show_path=False,
    show_level=False,
    show_time=False,
    rich_tracebacks=False,
    markup=True,
)

logger.propagate = False  # Don't log everything from the hierarchy.
logger.addHandler(rich_handler)
logger.setLevel(logging.INFO)
