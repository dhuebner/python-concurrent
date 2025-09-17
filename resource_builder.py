import asyncio
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import threading
from typing import Any, Optional
import uuid
import sys  # Ensure at top


class Resource:
    def __init__(self, content: str) -> None:
        self.content = content
        self.updated = datetime.now()

    def __str__(self) -> str:
        return f"Resource(content={self.content}, timestamp={self.updated.time()})"

    def __repr__(self) -> str:
        return self.__str__()


class BuildType(Enum):
    NOT_RUNNING = 0
    PARTIAL = 1
    FULL = 2


@dataclass
class BuildInfo:
    build_type: BuildType
    future: asyncio.Future[Resource]
    build_id: str
    is_superseded: bool = False


class ResourceBuilder:

    _instance: Optional['ResourceBuilder'] = None
    _lock = threading.Lock()
    _thread_local = threading.local()  # Shared per-thread storage for build_id

    def __new__(cls, *args: Any, **kwargs: Any) -> 'ResourceBuilder':
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(ResourceBuilder, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self) -> None:
        if hasattr(self, '_initialized'):
            return
        self._build_tasks: dict[str, BuildInfo] = {}
        self._progress_lock: threading.Lock = threading.Lock()
        self._superseded_ids = set()  # set of superseded build_ids
        self._initialized = True

    def build_state_of(self, uri: str) -> BuildType:
        if uri in self._build_tasks:
            return self._build_tasks[uri].build_type
        return BuildType.NOT_RUNNING

    def get_running_jobs(self) -> dict[str, str]:
        """Returns a dict mapping resource names to their truncated build IDs."""
        return {resource: info.build_id[-4:] for resource, info in self._build_tasks.items()}

    #
    # If force_rebuild is True, any ongoing build for the same resource is effectively cancelled and a new build is started.
    # The result of the new build is returned to waiters of the previous builds.
    #
    async def build_resource(self, uri: str, build_type: BuildType = BuildType.FULL, force_rebuild: bool = False) -> Resource:
        loop = asyncio.get_running_loop()
        if uri in self._build_tasks:
            if force_rebuild:
                return await self._force_rebuild(uri, build_type, loop)
            return await self._await_existing_build(uri)
        return await self._start_new_build(uri, build_type, loop)

    async def _force_rebuild(self, uri: str, build_type: BuildType, loop: asyncio.AbstractEventLoop) -> Resource:
        ResourceBuilder.report_progress(f"Force rebuild requested for: {uri}. Starting new build.")  # Use static call for consistency
        old_build_info = self._build_tasks.get(uri)
        if old_build_info and old_build_info.future and not old_build_info.future.done():
            old_build_info.is_superseded = True
            self._superseded_ids.add(old_build_info.build_id)
        new_build_id = str(uuid.uuid4())
        new_future: asyncio.Future[Resource] = loop.create_future()
        self._build_tasks[uri] = BuildInfo(build_type, new_future, new_build_id)

        # Chain old future to new one if exists
        if old_build_info and old_build_info.future and not old_build_info.future.done():
            def chain_result(done_future: asyncio.Future[Resource]) -> None:
                if not old_build_info.future.done():
                    try:
                        result = done_future.result()
                        old_build_info.future.set_result(result)
                    except Exception as e:
                        old_build_info.future.set_exception(e)
            new_future.add_done_callback(chain_result)

        # Start the new build in background
        asyncio.create_task(self._run_build(uri, build_type, new_build_id, loop))
        return await new_future

    async def _await_existing_build(self, uri: str) -> Resource:
        build_info = self._build_tasks[uri]
        return await build_info.future

    async def _start_new_build(self, uri: str, build_type: BuildType, loop: asyncio.AbstractEventLoop) -> Resource:
        ResourceBuilder.report_progress(f"Starting new build for: {uri} of type {build_type.name}")  # Use static call for consistency
        build_id = str(uuid.uuid4())
        future: asyncio.Future[Resource] = loop.create_future()
        self._build_tasks[uri] = BuildInfo(build_type, future, build_id)
        await self._run_build(uri, build_type, build_id, loop)
        return await future

    async def _run_build(self, uri: str, build_type: BuildType, build_id: str, loop: asyncio.AbstractEventLoop) -> None:
        try:
            def build_wrapper(uri: str, build_id: str) -> str:
                ResourceBuilder._thread_local.build_id = build_id
                res = ResourceBuilder._long_running_build(uri)
                ResourceBuilder._thread_local.build_id = None  # Reset to avoid carryover
                return res

            build_result = await loop.run_in_executor(None, build_wrapper, uri, build_id)
            result = Resource(build_result)
            result.updated = datetime.now()
            self._complete_build(uri, result, build_id)
        except Exception as e:
            # threads can not be interrupted, so we are using CancelledBuildException to simulate a cancelled build.
            if isinstance(e, CancelledBuildException) and self._build_tasks.get(uri) and self._build_tasks[uri].is_superseded:
                # Discard without failing the future; chaining will handle it
                with self._progress_lock:
                    if uri in self._build_tasks and self._build_tasks[uri].build_id == build_id:
                        del self._build_tasks[uri]  # Safe to del since future will be set by chaining
                    self._superseded_ids.discard(build_id)
            else:
                self._fail_build(uri, e, build_id)

    def _complete_build(self, uri: str, result: Resource, build_id: str) -> None:
        with self._progress_lock:
            if uri in self._build_tasks and self._build_tasks[uri].build_id == build_id:
                if self._build_tasks[uri].is_superseded:
                    del self._build_tasks[uri]
                    return
                build_info = self._build_tasks[uri]
                if not build_info.future.done():
                    build_info.future.set_result(result)
                del self._build_tasks[uri]
                self._superseded_ids.discard(build_id)

    def _fail_build(self, uri: str, exception: Exception, build_id: str) -> None:
        with self._progress_lock:
            if uri in self._build_tasks and self._build_tasks[uri].build_id == build_id:
                build_info = self._build_tasks[uri]
                if not build_info.future.done():
                    build_info.future.set_exception(exception)
                del self._build_tasks[uri]
            self._superseded_ids.discard(build_id)

    @staticmethod
    def report_progress(message: str) -> None:
        instance = ResourceBuilder()
        with instance._progress_lock:
            if hasattr(ResourceBuilder._thread_local, 'build_id') and ResourceBuilder._thread_local.build_id in instance._superseded_ids:
                # Extract uri for message
                uri = message.split(": ")[-1] if ": " in message else "unknown"
                raise CancelledBuildException(f"Build for {uri} cancelled due to force rebuild")
            
            thread_name = threading.current_thread().name
            timestamp = datetime.now().time()
            output = f"[{thread_name}][{timestamp}] {message}\n"
            sys.stdout.write(output)
            sys.stdout.flush()

    @staticmethod
    def _long_running_build(uri: str) -> str:
        # simulate high CPU load
        ResourceBuilder.report_progress(f"Building resource: {uri}")
        _res = 0
        max_iter = 10000 if uri == "A" else 7000
        for n in range(max_iter):
            _res = n**n
        ResourceBuilder.report_progress(f"Finished building resource: {uri}")
        return uri + "_built"


class CancelledBuildException(Exception):
    pass
