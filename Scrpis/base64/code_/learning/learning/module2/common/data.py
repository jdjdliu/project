from sdk.datasource import DataSource

__all__ = ["DataSource", "Outputs"]

VERSION = "v3"

class Outputs:

    def __init__(self, **kwargs):
        if "version" in kwargs:
            self.version = kwargs["version"]
            del kwargs["version"]
        else:
            self.version =VERSION

        for k, v in list(kwargs.items()):
            setattr(self, k, v)

    def get_attr(self, name, d=None):
        return self.__dict__.get(name, d)

    def __repr__(self):
        return str(self.__dict__)


if __name__ == "__main__":
    pass
