from datetime import datetime


class Resource:
    def __init__(self, uri: str, content: str = "") -> None:
        self.uri = uri
        self.content = content
        self.updated = datetime.now()

    def __str__(self) -> str:
        return f"Resource(uri={self.uri}, content={self.content}, timestamp={self.updated.time()})"

    def __repr__(self) -> str:
        return self.__str__()
