import asyncio
import threading
from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class Resource:
    def __str__(self) -> str:
        return f"Resource(content={self.content}, timestamp={self.timestamp.time()})"

    def __repr__(self) -> str:
        return self.__str__()

    def __init__(self, content: str) -> None:
        self.content = content
        self.timestamp = datetime.now()


class BuildType(Enum):
    NOT_RUNNING = 0
    PARTIAL = 1
    FULL = 2


@dataclass
class BuildInfo:
    build_type: BuildType
    future: asyncio.Future[Resource]


class ResourceBuilder:

    def __init__(self) -> None:
        self._build_tasks: dict[str, BuildInfo] = {}
        self._progress_lock = threading.Lock()

    def build_state_of(self, resource: str) -> BuildType:
        if resource in self._build_tasks:
            return self._build_tasks[resource].build_type
        return BuildType.NOT_RUNNING

    async def build_resource(self, resource: str, build_type: BuildType = BuildType.FULL) -> Resource:
        # If a build is already running, await its result
        if resource in self._build_tasks:
            self.report_progress(f"Build already in progress for: {resource}, awaiting result.")
            return await self._build_tasks[resource].future

        loop = asyncio.get_running_loop()
        future: asyncio.Future[Resource] = loop.create_future()
        self._build_tasks[resource] = BuildInfo(build_type, future)
        try:
            result = Resource(await loop.run_in_executor(None, self.long_running_build, resource))
            result.timestamp = datetime.now()
            future.set_result(result)
            return result
        except Exception as e:
            future.set_exception(e)
            raise
        finally:
            del self._build_tasks[resource]

    def long_running_build(self, resource: str) -> str:
        # simulate high CPU load
        self.report_progress(f"Building resource: {resource}")
        
        res = 0
        max = 10000 if resource == "A" else 7000
        for n in range(max):
            res = n**n
        return resource + "_built"

    def report_progress(self, str) -> None:
        with self._progress_lock:
            print(f"[{threading.current_thread().name}][{datetime.now().time()}] {str}")
