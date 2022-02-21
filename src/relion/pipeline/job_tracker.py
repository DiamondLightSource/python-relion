from pathlib import Path
from typing import Any, Dict, Union


class JobTracker:
    def __init__(self):
        self._job_paths: Dict[str, Union[str, Dict[str, str]]] = {}

    def __getitem__(self, key: str) -> Union[str, Dict[str, str]]:
        return self._job_paths[key]

    def __setitem__(self, key: str, value: Union[str, Dict[str, str]]):
        if not isinstance(value, str) and not isinstance(value, dict):
            raise TypeError(
                f"Only a string or dictionary may be used as a value in JobTracker, not {type(value)}"
            )
        self._job_paths[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        return self._job_paths.get(key, default)

    def get_str(self, key: str) -> str:
        if isinstance(self._job_paths[key], str):
            return self._job_paths[key]
        raise TypeError(f"The key {key} does not get a string from JobTracker {self}")

    def get_member(self, key: str, second_key: str, validate: bool = True) -> str:
        if isinstance(self._job_paths[key], str):
            if validate:
                raise TypeError(
                    f"The key {key} returned a string from get_member method of JobTracker"
                )
            return ""
        return self._job_paths[key].get(second_key)

    def set_member(
        self, key: str, second_key: str, value: str, validate: bool = True
    ) -> None:
        if isinstance(self._job_paths[key], str):
            if validate:
                raise TypeError(
                    f"The key {key} returned a string from get_member method of JobTracker"
                )
            return None
        self._job_paths[key][second_key] = value
        return None

    def path_to(self, key: str, filename: str) -> Path:
        try:
            if not isinstance(self._job_paths[key], str):
                raise ValueError(
                    f"{key} does not return a string from the job tracker: {self._job_paths[key]}"
                )
        except KeyError as e:
            raise KeyError(f"KeyError raised in JobTracker path_to method: {e}")
        return Path(self._job_paths[key]) / filename
