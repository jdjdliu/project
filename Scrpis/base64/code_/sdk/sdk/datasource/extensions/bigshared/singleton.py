import threading


# 用于非多线程安全单例，每个线程会有一个实例
class WithThreadLocalInstance:
    @classmethod
    def instance(cls):
        if not hasattr(cls, "__THREAD_LOCAL__"):
            cls.__THREAD_LOCAL__ = threading.local()
        if not hasattr(cls.__THREAD_LOCAL__, "instance"):
            cls.__THREAD_LOCAL__.__setattr__("instance", cls())
        return cls.__THREAD_LOCAL__.instance


# 用于多线程安全的单例
class WithThreadSafeInstance:
    @classmethod
    def instance(cls):
        if not hasattr(cls, "_INSTANCE"):
            cls._INSTANCE = cls()
        return cls._INSTANCE
