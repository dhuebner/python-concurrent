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
    cancel_event: asyncio.Event


class ResourceBuilder:

    def __init__(self) -> None:
        self._build_tasks: dict[str, BuildInfo] = {}
        self._progress_lock = threading.Lock()

    def build_state_of(self, resource: str) -> BuildType:
        if resource in self._build_tasks:
            return self._build_tasks[resource].build_type
        return BuildType.NOT_RUNNING

    async def build_resource(self, resource: str, build_type: BuildType = BuildType.FULL, force_rebuild: bool = False) -> Resource:
        loop = asyncio.get_running_loop()
        # Always use the latest future for all requests
        while True:
            if resource in self._build_tasks:
                build_info = self._build_tasks[resource]
                if force_rebuild:
                    self.report_progress(f"Force rebuild requested for: {resource}. Cancelling current build.")
                    self._cancel_build(resource)
                    # Immediately start the new build after cancellation
                    cancel_event = asyncio.Event()
                    build_info.cancel_event = cancel_event
                    build_result = await loop.run_in_executor(None, ResourceBuilder._long_running_build, resource, cancel_event, self)
                    if build_result is None or (isinstance(build_result, str) and build_result == "__CANCELLED__"):
                        continue
                    result = Resource(build_result)
                    result.timestamp = datetime.now()
                    build_info.future.set_result(result)
                    del self._build_tasks[resource]
                    return result
                else:
                    build_result = await build_info.future
                    # If the build was cancelled, loop and await the new future
                    if isinstance(build_result, Resource):
                        return build_result
                    elif build_result is None or (isinstance(build_result, str) and build_result == "__CANCELLED__"):
                        continue
                    else:
                        raise Exception(f"Unexpected build result for {resource}: {build_result}")
            else:
                self.report_progress(f"Starting new build for: {resource} of type {build_type.name}")
                future: asyncio.Future[Resource] = loop.create_future()
                cancel_event = asyncio.Event()
                self._build_tasks[resource] = BuildInfo(build_type, future, cancel_event)
                build_result = await loop.run_in_executor(None, ResourceBuilder._long_running_build, resource, cancel_event, self)
                if build_result is None or (isinstance(build_result, str) and build_result == "__CANCELLED__"):
                    continue
                result = Resource(build_result)
                result.timestamp = datetime.now()
                future.set_result(result)
                del self._build_tasks[resource]
                return result

    @staticmethod
    def _long_running_build(resource: str, cancel_event, builder_instance) -> str:
        # simulate high CPU load
        builder_instance.report_progress(f"Building resource: {resource}")
        res = 0
        max = 10000 if resource == "A" else 7000
        for n in range(max):
            if cancel_event.is_set():
                builder_instance.report_progress(f"Build cancelled for: {resource}")
                return "__CANCELLED__"
            res = n**n
        return resource + "_built"

    def _cancel_build(self, resource: str) -> None:
        """Cancel the build for a specific resource."""
        build_info = self._build_tasks.get(resource)
        if build_info and build_info.cancel_event:
            build_info.cancel_event.set()
            self.report_progress(f"Cancellation requested for resource: {resource}")

    def report_progress(self, str) -> None:
        with self._progress_lock:
            print(f"[{threading.current_thread().name}][{datetime.now().time()}] {str}")
