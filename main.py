import asyncio

from resource_manager import ResourceManager


async def print_ticks(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        print("Tick")
        await asyncio.sleep(0.5)


async def execute() -> None:
    manager = ResourceManager()
    resources = ["A", "B", "A", "C"]
    stop_event = asyncio.Event()

    async def build_all():
        # Request all resources in parallel
        tasks = [manager.get_or_add_resource(resource) for resource in resources]
        for coro in asyncio.as_completed(tasks):
            built_res = await coro
            # Find which resource this result belongs to
            # (Assumes built_res is unique or you can map it back)
            print(f"Got resource: {built_res}")
        stop_event.set()
        print(f"Built resources: {manager.built_resources}")

    await asyncio.gather(print_ticks(stop_event), build_all())


if __name__ == "__main__":
    asyncio.run(execute())
