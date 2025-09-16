import asyncio
from dataclasses import dataclass
from enum import Enum


class BuildType(Enum):
    NOT_RUNNING = 0
    PARTIAL = 1
    FULL = 2


@dataclass
class BuildInfo:
    build_type: BuildType
    future: asyncio.Future[str]


class ResourceBuilder:
    def __init__(self) -> None:
        self._build_tasks: dict[str, BuildInfo] = {}

    def build_state_of(self, resource: str) -> BuildType:
        if resource in self._build_tasks:
            return self._build_tasks[resource].build_type
        return BuildType.NOT_RUNNING

    async def build_resource(
        self, resource: str, build_type: BuildType = BuildType.FULL
    ) -> str:
        print(f"Building resource: {resource}")
        # If a build is already running, await its result
        if resource in self._build_tasks:
            print(f"Build already in progress for: {resource}, awaiting result.")
            return await self._build_tasks[resource].future

        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()
        self._build_tasks[resource] = BuildInfo(build_type, future)
        try:
            result = await loop.run_in_executor(None, self.long_running_build, resource)
            future.set_result(result)
            return result
        except Exception as e:
            future.set_exception(e)
            raise
        finally:
            del self._build_tasks[resource]

    def long_running_build(self, resource: str) -> str:
        print(f"Starting long-running build for: {resource}")
        # simulate high CPU load
        res = 0
        max = 10000 if resource == "A" else 7000
        for n in range(max):
            res = n**n
        print(f"Finished long-running build for: {resource}")
        return resource + "_built"
