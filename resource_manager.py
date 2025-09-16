import asyncio
import threading
import time
from typing import List

from resource_builder import ResourceBuilder


class ResourceManager:
    def __init__(self) -> None:
        self.builder = ResourceBuilder()
        self.built_resources: dict[str, str] = {}

    async def get_resource(self, resource: str) -> str | None:
        print(f"Requesting resource: {resource}")
        if resource in self.built_resources:
            return self.built_resources.get(resource)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.builder.build_resource, resource)
