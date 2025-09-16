import asyncio
import time


class ResourceBuilder:
    def __init__(self) -> None:
        self.built_resources: dict[str, str] = {}

    async def build_resource(self, resource: str) -> str:
        print(f"Building resource: {resource}")
        return await self.long_running_build(resource)


    async def long_running_build(self, resource: str) -> str:
        print(f"Starting long-running build for: {resource}")
        await asyncio.sleep(5)  # Simulate a longer build time
        print(f"Finished long-running build for: {resource}")
        return resource + "_built"
