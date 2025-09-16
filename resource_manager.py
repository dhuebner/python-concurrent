from resource_builder import ResourceBuilder


class ResourceManager:

    def __init__(self) -> None:
        self.builder = ResourceBuilder()
        self.built_resources: dict[str, str] = {}

    async def get_or_add_resource(self, resource: str, rebuild: bool = False) -> str | None:
        print(f"Requesting resource: {resource}")
        if not rebuild and resource in self.built_resources:
            return self.built_resources.get(resource)

        if rebuild:
            print(f"Rebuilding resource: {resource}")
        built_res = await self.builder.build_resource(resource)
        self.built_resources[resource] = built_res
        return self.built_resources.get(resource)
