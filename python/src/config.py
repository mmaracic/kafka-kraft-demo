from __future__ import annotations

from typing import Any, ClassVar, Sequence, Type, TypeVar, Union, Optional

from pydantic import BaseModel, model_validator


# If constructor is invoked with a parameter of this name then extraction is done
WRAPPER_INVOKE_PATH: str = "source"


T = TypeVar("T", bound="BaseConfig")


class BaseConfig(BaseModel):
    """
    Base configuration model with support for wrapper-based constructor extraction.
    Subclasses may define `WRAPPER_DATA_PATH` to enable automatic extraction of
    nested configuration data when the constructor is invoked with a dictionary
    containing that data under a specific key and the constructor is called using the
    key defined in `WRAPPER_INVOKE_PATH`.
    """

    model_config = {"populate_by_name": True}

    # Optional class variable that points what is the position of extracted data
    # in the input dictionary given to the constructor.
    WRAPPER_DATA_PATH: ClassVar[Optional[str]] = None

    def model_dump(self, *args, **kwargs):
        kwargs.setdefault("by_alias", True)
        return super().model_dump(*args, **kwargs)

    @staticmethod
    def _select_node(
        node: dict[str, Any], path: Union[str, Sequence[str]]
    ) -> dict[str, Any]:
        """Traverse `node` and return the sub-node selected by `path`.

        `path` may be a dot-separated string like "a.b.0.c" or a sequence of keys.
        Numeric tokens index lists.
        """
        if isinstance(path, str):
            parts: list[str] = [p for p in path.split(".") if p != ""]
        else:
            parts = list(path)

        cur: Any = node
        parts.insert(0, "source")
        for token in parts:
            if isinstance(cur, list):
                idx = int(token)
                cur = cur[idx]
            elif isinstance(cur, dict):
                cur = cur[token]
            else:
                raise KeyError(
                    f"Cannot traverse into {type(cur)!r} with token {token!r}"
                )
        return cur

    @model_validator(mode="before")
    def _maybe_extract_from_wrapper(cls, values: dict[str, Any]) -> dict[str, Any]:  # type: ignore[override]
        """
        If `WRAPPER_DATA_PATH` is set, extract the nested node from `values`
        using that path.
        The constructor must be invoked using the key defined in `WRAPPER_INVOKE_PATH`.
        """
        path_key = getattr(cls, "WRAPPER_DATA_PATH", None)
        if path_key and WRAPPER_INVOKE_PATH in values.keys():
            try:
                node = cls._select_node(values, path_key)
            except Exception as exc:
                raise ValueError(
                    f"invalid path {values.get(path_key)!r}: {exc}"
                ) from exc
            return node

        return values

    def from_yaml(cls: Type[T], path: str) -> T:
        """Load configuration from a YAML file."""
        import yaml

        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)