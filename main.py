import asyncio

from resource_manager import ResourceManager
from resource_builder import ResourceBuilder


async def print_ticks(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        # Print the current state of running build jobs
        running_jobs = ResourceBuilder().get_running_jobs()
        print(f" Running jobs: {running_jobs}")
        await asyncio.sleep(0.5)


async def execute() -> None:
    manager = ResourceManager()
    stop_event = asyncio.Event()
    b_started_event = asyncio.Event()

    # Patch ResourceBuilder to signal when B build starts
    orig_report_progress = ResourceBuilder.report_progress

    def patched_report_progress(uri: str, message: str, progress: int) -> None:
        if message.startswith("Building resource: B"):
            b_started_event.set()
        orig_report_progress(uri, message, progress)

    ResourceBuilder.report_progress = patched_report_progress  # Class-level patch for static method

    async def build_all():
        # Create build tasks for each resource
        task_a = asyncio.create_task(manager.get_or_add_resource("A", False))
        task_b = asyncio.create_task(manager.get_or_add_resource("B", False))
        task_c = asyncio.create_task(manager.get_or_add_resource("C", False))
        # Wait until B's build has actually started
        await b_started_event.wait()
        await asyncio.sleep(2)
        task_a_2 = asyncio.create_task(manager.get_or_add_resource("A", False))
        task_b_rebuild = asyncio.create_task(manager.get_or_add_resource("B", True))

        for coro in asyncio.as_completed([task_a, task_b, task_c, task_b_rebuild, task_a_2]):
            try:
                result = await coro
                # Find the name for this task
                print(f"Got resource: {result}")
            except Exception as e:
                print(f"Build failed: {e}")
        stop_event.set()

    await asyncio.gather(print_ticks(stop_event), build_all())

    print("Built resources:")
    for name, resource in manager.built_resources.items():
        print(f"\t{name}: {resource}")


if __name__ == "__main__":
    asyncio.run(execute())
