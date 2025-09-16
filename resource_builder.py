import asyncio
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import threading


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
        self._build_tasks = {}
        self._progress_lock = threading.Lock()  # Replaced asyncio.Lock with threading.Lock

    def build_state_of(self, uri: str) -> BuildType:
        if uri in self._build_tasks:
            return self._build_tasks[uri].build_type
        return BuildType.NOT_RUNNING

    #
    # If force_rebuild is True, any ongoing build for the same resource is ignored and a new build is started.
    # The result of the new build is returned to waiters of the previous build.
    #
    async def build_resource(self, uri: str, build_type: BuildType = BuildType.FULL, force_rebuild: bool = False) -> Resource:
        loop = asyncio.get_running_loop()
        if uri in self._build_tasks:
            if force_rebuild:
                return await self._force_rebuild(uri, build_type, loop)
            return await self._await_existing_build(uri)
        return await self._start_new_build(uri, build_type, loop)

    async def _force_rebuild(self, uri: str, build_type: BuildType, loop) -> Resource:
        self.report_progress(f"Force rebuild requested for: {uri}. Starting new build.")
        future: asyncio.Future[Resource] = loop.create_future()
        self._build_tasks[uri] = BuildInfo(build_type, future)
        return await self._await_existing_build(uri)

    async def _await_existing_build(self, uri: str) -> Resource:
        build_info = self._build_tasks[uri]
        build_result = await build_info.future
        if isinstance(build_result, Resource):
            return build_result
        elif build_result is None:
            self.report_progress(f"Stop and await rebuild of {uri}.")
            return await self.build_resource(uri)
        else:
            raise Exception(f"Unexpected build result for {uri}: {build_result}")

    async def _start_new_build(self, uri: str, build_type: BuildType, loop) -> Resource:
        self.report_progress(f"Starting new build for: {uri} of type {build_type.name}")
        future: asyncio.Future[Resource] = loop.create_future()
        self._build_tasks[uri] = BuildInfo(build_type, future)
        build_result = await loop.run_in_executor(None, ResourceBuilder._long_running_build, uri, self)
        if build_result is None:
            return await self.build_resource(uri, build_type)
        result = Resource(build_result)
        result.timestamp = datetime.now()
        self._complete_build(uri, result)
        return result

    def _complete_build(self, uri: str, result: Resource) -> None:
        if uri in self._build_tasks:
            build_info = self._build_tasks[uri]
            if build_info.future and not build_info.future.done():
                build_info.future.set_result(result)
            del self._build_tasks[uri]

    def report_progress(self, message: str) -> None:
        with self._progress_lock:
            thread_name = threading.current_thread().name
            print(f"[{thread_name}][{datetime.now().time()}] {message}")

    @staticmethod
    def _long_running_build(uri: str, builder_instance: 'ResourceBuilder') -> str:
        # simulate high CPU load
        builder_instance.report_progress(f"Building resource: {uri}")
        res = 0
        max = 10000 if uri == "A" else 7000
        for n in range(max):
            res = n**n
        builder_instance.report_progress(f"Finished building resource: {uri}")
        return uri + "_built"
