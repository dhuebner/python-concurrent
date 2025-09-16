import asyncio
import resource

from resource_manager import ResourceManager


async def print_ticks(stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        print("Tick")
        await asyncio.sleep(0.5)


async def execute() -> None:
    manager = ResourceManager()
    resources: list[tuple[str, bool]] = [("A", False), ("B", False), ("A", False), ("C", False),  ("B", True)]
    stop_event = asyncio.Event()

    async def build_all():
        # Request all resources in parallel
        tasks = [manager.get_or_add_resource(resource, rebuild) for resource, rebuild in resources]
        for coro in asyncio.as_completed(tasks):
            built_res = await coro
            print(f"Got resource: {built_res}")
        stop_event.set()

    await asyncio.gather(print_ticks(stop_event), build_all())
    
    print("Built resources:")
    for name, resource in manager.built_resources.items():
        print(f"\t{name}: {resource}")


if __name__ == "__main__":
    asyncio.run(execute())
