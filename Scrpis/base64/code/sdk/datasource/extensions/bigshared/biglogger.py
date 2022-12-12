import datetime
import logging
import os
import sys
import time


class BigFormatter(logging.Formatter):
    def __init__(self, format="[%(asctime)s] %(levelname)s %(name)s: %(message)s"):
        super().__init__(fmt="%(levelno)d: %(msg)s", datefmt=None, style="%")
        self._fmt = format

    def format(self, record):
        """
        Format the specified record as text.

        The record's attribute dictionary is used as the operand to a
        string formatting operation which yields the returned string.
        Before formatting the dictionary, a couple of preparatory steps
        are carried out. The message attribute of the record is computed
        using LogRecord.getMessage(). If the formatting string uses the
        time (as determined by a call to usesTime(), formatTime() is
        called to format the event time. If there is exception information,
        it is formatted using formatException() and appended to the message.
        """
        self._style._fmt = self._fmt  # add self format
        record.message = record.getMessage()
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)
        s = self.formatMessage(record)
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            if s[-1:] != "\n":
                s = s + "\n"
            s = s + record.exc_text
        if record.stack_info:
            if s[-1:] != "\n":
                s = s + "\n"
            s = s + self.formatStack(record.stack_info)
        return s

    def formatTime(self, record, datefmt=None):
        """
        Return the creation time of the specified LogRecord as formatted text.

        This method should be called from format() by a formatter which
        wants to make use of a formatted time. This method can be overridden
        in formatters to provide for any specific requirement, but the
        basic behaviour is as follows: if datefmt (a string) is specified,
        it is used with time.strftime() to format the creation time of the
        record. Otherwise, the ISO8601 format is used. The resulting
        string is returned. This function uses a user-configurable function
        to convert the creation time to a tuple. By default, time.localtime()
        is used; to change this for a particular formatter instance, set the
        'converter' attribute to a function with the same signature as
        time.localtime() or time.gmtime(). To change it for all formatters,
        for example if you want all logging times to be shown in GMT,
        set the 'converter' attribute in the Formatter class.
        """
        ct = self.converter(record.created)
        if datefmt:
            s = time.strftime(datefmt, ct)
        else:
            # t = time.strftime("%Y-%m-%d %H:%M:%S", ct)
            # s = "%s,%03d" % (t, record.msecs)
            s = str(datetime.datetime.now())  # add self format

        return s


class BigLogger:
    def __init__(self, log_name=""):
        self.level = int(os.getenv("LOGGING_LEVEL", logging.INFO))
        self.log_name = log_name
        self.log = self.get_logger(self.log_name)
        self.log.setLevel(self.level)

    def get_logger(self, log_name):
        log = logging.getLogger(log_name)
        if not log.handlers:
            handler = logging.StreamHandler(sys.stdout)
            fmt = BigFormatter(format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")
            handler.setFormatter(fmt)
            log.addHandler(handler)
        return log

    def set_level(self, level):
        self.level = level

    def error(self, content):
        if self.level > logging.ERROR:
            return
        if os.environ.get("DisplayLog"):
            self._display(content, "ERROR")
        else:
            self.log.error(content)

    def info(self, content):
        if self.level > logging.INFO:
            return
        if os.environ.get("DisplayLog"):
            self._display(content, "INFO")
        else:
            self.log.info(content)

    def warn(self, content):
        if self.level > logging.WARNING:
            return
        if os.environ.get("DisplayLog"):
            self._display(content, "WARNING")
        else:
            self.log.warn(content)

    def warning(self, content):
        if self.level > logging.WARNING:
            return
        if os.environ.get("DisplayLog"):
            self._display(content, "WARNING")
        else:
            self.log.warning(content)

    def debug(self, content):
        if self.level > logging.DEBUG:
            return
        if os.environ.get("DisplayLog"):
            self._display(content, "DEBUG")
        else:
            self.log.debug(content)

    def exception(self, content):
        if os.environ.get("DisplayLog"):
            self._display(content, "EXCEPTION")
        else:
            self.log.exception(content)


# if __name__ == '__main__':
#     os.environ['DisplayLog']='DisplayLog'
#     start_time = datetime.datetime.now()
#     log = BigLogger('bigquant')
#     for i in range(10000):
#         log.info("info")
#     print((datetime.datetime.now() - start_time).total_seconds())
