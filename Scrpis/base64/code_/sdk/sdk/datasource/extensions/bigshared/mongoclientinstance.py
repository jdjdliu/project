from pymongo import MongoClient

from .singleton import WithThreadSafeInstance

try:

    class MongoClientInstance(MongoClient, WithThreadSafeInstance):
        def __init__(self):
            from sdk.datasource.extensions.bigshared import settings

            MongoClient.__init__(self, settings.mongodb_conf, connect=False)


except TypeError:
    # TypeError: metaclass conflict
    # only for unittest: MongoClient got patched
    MongoClientInstance = MongoClient

    def instance():
        from sdk.datasource.extensions.bigshared import settings

        return MongoClient(settings.mongodb_conf, connect=False)

    MongoClientInstance.instance = instance
