import dataclasses
import json
import logging
import abc


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class DispatcherSettings(abc.ABC):
    durable: bool = True


class Dispatcher:
    def __init__(
        self,
        dsn: str,
        exchange_name: str,
        settings: DispatcherSettings | None = None,
        json_encoder: json.JSONEncoder | None = None,
        logger: logging.Logger | None = None,
    ):
        self._dsn = dsn
        self._exchange_name = exchange_name
        self._settings = settings
        self._logger = logger
        self._json_encoder = json_encoder
        
 
