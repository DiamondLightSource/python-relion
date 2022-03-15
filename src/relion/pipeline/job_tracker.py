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
        stringy_return = self._job_paths[key]
        if isinstance(stringy_return, str):
            return stringy_return
        raise TypeError(f"The key {key} does not get a string from JobTracker {self}")

    def get_member(self, key: str, second_key: str, validate: bool = True) -> str:
        job_path = self._job_paths[key]
        if isinstance(job_path, str):
            if validate:
                raise TypeError(
                    f"The key {key} returned a string from get_member method of JobTracker"
                )
            return ""
        return job_path[second_key]

    def set_member(
        self, key: str, second_key: str, value: str, validate: bool = True
    ) -> None:
        job_path = self._job_paths[key]
        if isinstance(job_path, str):
            if validate:
                raise TypeError(
                    f"The key {key} returned a string from get_member method of JobTracker"
                )
            return None
        job_path[second_key] = value
        return None

    def path_to(self, key: str, filename: str) -> Path:
        try:
            job_path = self._job_paths[key]
            if not isinstance(job_path, str):
                raise ValueError(
                    f"{key} does not return a string from the job tracker: {self._job_paths[key]}"
                )
        except KeyError as e:
            raise KeyError(f"KeyError raised in JobTracker path_to method: {e}")
        return Path(job_path) / filename
