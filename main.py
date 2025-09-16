import asyncio

from resource_manager import ResourceManager


async def execute() -> None:
    manager = ResourceManager()
    resources = ["A", "B", "C", "D"]
    for resource in resources:
        built_res = await manager.get_resource(resource)
        print(f"Resource {resource}: {built_res}")
    print(f"Built resources: {manager.built_resources}")


if __name__ == "__main__":
    asyncio.run(execute())
