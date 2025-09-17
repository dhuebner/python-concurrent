import threading
from typing import Any, Optional
from resource_builder import ResourceBuilder, BuilderOptions
from resource_types import Resource

class ResourceManager:
    _instance: Optional["ResourceManager"] = None
    _lock = threading.Lock()

    def __new__(cls, *args: Any, **kwargs: Any) -> "ResourceManager":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(ResourceManager, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self) -> None:
        if hasattr(self, "_initialized"):
            return
        self.builder = ResourceBuilder()
        self.built_resources: dict[str, Resource] = {}
        self._initialized = True

    async def get_or_add_resource(self, uri: str, rebuild: bool = False) -> Resource | None:
        if not rebuild and uri in self.built_resources:
            return self.built_resources[uri]

        built_res = await self.builder.build_resource(uri, BuilderOptions(force_rebuild=rebuild))
        self.built_resources[uri] = built_res
        return built_res
