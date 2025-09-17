import asyncio
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import threading
from typing import Any, Optional
import uuid
import sys

from resource_types import Resource


class BuildType(Enum):
    PARTIAL = 0
    FULL = 1


class BuildState:
    def __init__(self, build_type: BuildType, progress: int = 0, message: str = "Not started"):
        self.build_type = build_type
        self.progress = max(0, min(100, progress))
        self.message = message

    def __str__(self) -> str:
        return f"BuildState(build_type={self.build_type.name}, progress={self.progress}, message={self.message})"

    def __repr__(self) -> str:
        return self.__str__()

    def set_progress(self, progress: int, message: str) -> None:
        self.progress = max(0, min(100, progress))
        self.message = message


@dataclass
class BuildInfo:
    build_state: BuildState
    future: asyncio.Future[Resource]
    build_id: str
    is_superseded: bool = False


@dataclass
class BuilderOptions:
    build_type: BuildType = BuildType.FULL
    force_rebuild: bool = False


class CancelledBuildException(Exception):
    pass


class ResourceBuilder:

    _instance: Optional["ResourceBuilder"] = None
    _lock = threading.Lock()
    _thread_local = threading.local()  # Shared per-thread storage for build_id

    def __new__(cls, *args: Any, **kwargs: Any) -> "ResourceBuilder":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(ResourceBuilder, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self) -> None:
        if hasattr(self, "_initialized"):
            return
        self._build_tasks: dict[str, BuildInfo] = {}
        self._progress_lock: threading.Lock = threading.Lock()
        self._superseded_ids = set()
        self._initialized = True

    def build_state_of(self, uri: str) -> BuildState | None:
        if uri in self._build_tasks:
            return self._build_tasks[uri].build_state
        return None

    def get_running_jobs(self) -> dict[str, str]:
        """Returns a dict mapping resource names to their truncated build IDs."""
        with self._progress_lock:
            return {resource: info.build_id[-4:] for resource, info in self._build_tasks.items()}  # Locked snapshot

    def cancel_running(self, uri: str) -> bool:
        """Attempts to cancel the running build for the given URI.

        Returns True if a running build was found and marked for cancellation, False otherwise.
        Waiters will receive a CancelledBuildException.
        The build thread will stop at the next progress checkpoint.
        """
        with self._progress_lock:
            if uri not in self._build_tasks:
                return False
            build_info = self._build_tasks[uri]
            if build_info.future.done():
                return False

            build_info.is_superseded = True
            self._superseded_ids.add(build_info.build_id)

            # Immediately notify waiters
            if not build_info.future.done():
                build_info.future.set_exception(CancelledBuildException(f"Build for {uri} cancelled"))

            # Do NOT delete from _build_tasks here - let _run_build cleanup handle it
            return True

    #
    # If force_rebuild is True, any ongoing build for the same resource is effectively cancelled and a new build is started.
    # The result of the new build is returned to waiters of the previous builds.
    #
    async def build_resource(self, uri: str, options: BuilderOptions = BuilderOptions()) -> Resource:
        loop = asyncio.get_running_loop()
        if uri in self._build_tasks:
            if options.force_rebuild:
                return await self._force_rebuild(uri, options, loop)
            return await self._await_existing_build(uri)
        return await self._start_new_build(uri, options, loop)

    async def _force_rebuild(self, uri: str, options: BuilderOptions, loop: asyncio.AbstractEventLoop) -> Resource:
        ResourceBuilder.report_progress(uri, f"Force rebuild requested for: {uri}. Starting new build.", 0)
        with self._progress_lock:
            old_build_info = self._build_tasks.get(uri)
            should_flag_and_chain = old_build_info and not old_build_info.future.done()
            if old_build_info and should_flag_and_chain:
                old_build_info.is_superseded = True
                self._superseded_ids.add(old_build_info.build_id)
        new_build_id = str(uuid.uuid4())
        new_future: asyncio.Future[Resource] = loop.create_future()
        self._build_tasks[uri] = BuildInfo(BuildState(options.build_type), new_future, new_build_id)

        # Chain old future to new one if exists
        if should_flag_and_chain:

            def chain_result(done_future: asyncio.Future[Resource]) -> None:
                # Propagate result of new build to the waiting one.
                if old_build_info and not old_build_info.future.done():
                    try:
                        old_build_info.future.set_result(done_future.result())
                    except Exception as e:
                        old_build_info.future.set_exception(e)

            new_future.add_done_callback(chain_result)

        # Start the new build in background with options
        asyncio.create_task(self._run_build(uri, options, new_build_id, loop))
        return await new_future

    async def _await_existing_build(self, uri: str) -> Resource:
        build_info = self._build_tasks[uri]
        return await build_info.future

    async def _start_new_build(self, uri: str, options: BuilderOptions, loop: asyncio.AbstractEventLoop) -> Resource:
        ResourceBuilder.report_progress(uri, f"Starting new build for: {uri} of type {options.build_type.name}", 0)
        build_id = str(uuid.uuid4())
        future: asyncio.Future[Resource] = loop.create_future()
        self._build_tasks[uri] = BuildInfo(BuildState(options.build_type), future, build_id)
        await self._run_build(uri, options, build_id, loop)
        return await future

    async def _run_build(self, uri: str, options: BuilderOptions, build_id: str, loop: asyncio.AbstractEventLoop) -> None:
        try:

            def build_wrapper(resource: Resource, build_id: str, options: BuilderOptions) -> None:
                # Make sure to associate the build with a thread.
                ResourceBuilder._thread_local.build_id = build_id
                ResourceBuilder._buildResource(resource, options)
                ResourceBuilder._thread_local.build_id = None  # Reset to avoid carryover
                return

            resource = Resource(uri)
            await loop.run_in_executor(None, build_wrapper, resource, build_id, options)
            resource.updated = datetime.now()
            self._complete_build(uri, resource, build_id)
        except Exception as e:
            if not isinstance(e, CancelledBuildException):
                self._fail_build(uri, e, build_id)
                return
            build_info = self._build_tasks.get(uri)
            if build_info is None or not build_info.is_superseded:
                self._fail_build(uri, e, build_id)
                return
            with self._progress_lock:
                if self._validate_current_build(uri, build_id):
                    del self._build_tasks[uri]
                self._superseded_ids.discard(build_id)
        finally:
            self._superseded_ids.discard(build_id)

    def _complete_build(self, uri: str, result: Resource, build_id: str) -> None:
        with self._progress_lock:
            if self._validate_current_build(uri, build_id):
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
            if self._validate_current_build(uri, build_id):
                build_info = self._build_tasks[uri]
                if not build_info.future.done():
                    build_info.future.set_exception(exception)
                del self._build_tasks[uri]
            self._superseded_ids.discard(build_id)

    def _validate_current_build(self, uri: str, build_id: str) -> bool:
        """Helper to check if this is the current build for the URI."""
        return uri in self._build_tasks and self._build_tasks[uri].build_id == build_id

    @staticmethod
    def report_progress(uri: str, message: str, progress: int) -> None:
        instance = ResourceBuilder()
        with instance._progress_lock:
            if hasattr(ResourceBuilder._thread_local, "build_id"):
                build_id = ResourceBuilder._thread_local.build_id
                if build_id in instance._superseded_ids:
                    raise CancelledBuildException(f"Build for {uri} cancelled due to force rebuild")

                build_info = instance._build_tasks.get(uri)
                if build_info:
                    build_info.build_state.set_progress(progress, message)

            thread_name = threading.current_thread().name
            timestamp = datetime.now().time()
            output = f"[{thread_name}][{timestamp}] {message}\n"
            sys.stdout.write(output)
            sys.stdout.flush()

    @staticmethod
    def _buildResource(origin: Resource, options: BuilderOptions) -> None:
        uri = origin.uri
        # simulate high CPU load, now using options (e.g., adjust based on build_type)
        ResourceBuilder.report_progress(uri, f"Building resource: {uri}", 0)

        # Example: Call an async function synchronously
        async def some_async_helper() -> str:  # This is just a placeholder—replace with your real async function
            await asyncio.sleep(1)  # Simulate some async work
            return "async_result"

        try:
            asyncio.run(some_async_helper())
        except Exception as e:
            print(f"Async call failed: {e}")

        _res = 0
        max_iter = 10000 if uri == "A" else 7000  # Could adjust based on options.build_type
        for n in range(max_iter):
            _res = n**n
            if n % (max_iter // 10) == 0:  # Report every 10%
                progress = int((n / max_iter) * 100)
                ResourceBuilder.report_progress(uri, f"Progress for {uri}: {progress}%", progress)
        ResourceBuilder.report_progress(uri, f"Finished building resource: {uri}", 100)
        origin.content = uri + "_built"
        return
