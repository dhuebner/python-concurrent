import asyncio
import resource

from resource_manager import ResourceManager


async def print_ticks(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        print("Tick")
        await asyncio.sleep(0.5)


async def execute() -> None:
    manager = ResourceManager()
    stop_event = asyncio.Event()
    b_started_event = asyncio.Event()

    # Patch ResourceBuilder to signal when B build starts
    orig_report_progress = manager.builder.report_progress

    def patched_report_progress(str):
        if str.startswith("Building resource: B"):
            b_started_event.set()
        orig_report_progress(str)

    manager.builder.report_progress = patched_report_progress

    async def build_all():
        tasks = []
        tasks.append(asyncio.create_task(manager.get_or_add_resource("A", False)))
        tasks.append(asyncio.create_task(manager.get_or_add_resource("B", False)))
        tasks.append(asyncio.create_task(manager.get_or_add_resource("C", False)))
        # Wait until B's build has actually started
        await b_started_event.wait()
        tasks.append(asyncio.create_task(manager.get_or_add_resource("B", True)))
        # Await all tasks as they complete
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for built_res in results:
                if isinstance(built_res, Exception):
                    print(f"Build failed: {built_res}")
                else:
                    print(f"Got resource: {built_res}")
        finally:
            stop_event.set()

    await asyncio.gather(print_ticks(stop_event), build_all())

    print("Built resources:")
    for name, resource in manager.built_resources.items():
        print(f"\t{name}: {resource}")


if __name__ == "__main__":
    asyncio.run(execute())
