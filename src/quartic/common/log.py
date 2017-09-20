import sys
import logging

class InfoFilter(logging.Filter):
    def filter(self, record):
        return record.levelno in (logging.DEBUG, logging.INFO)


FORMAT = "%(levelname)-7s %(message)s"
DATE_FORMAT = "%b %d %H:%M:%S"
formatter = logging.Formatter(fmt=FORMAT, datefmt=DATE_FORMAT)

def logger(name):
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)

    h1 = logging.StreamHandler(sys.stdout)
    h1.setLevel(logging.DEBUG)
    h1.addFilter(InfoFilter())
    h1.setFormatter(formatter)

    h2 = logging.StreamHandler()
    h2.setLevel(logging.WARNING)
    h2.setFormatter(formatter)

    log.addHandler(h1)
    log.addHandler(h2)
    return log

if __name__ == "__main__":
    my_log = logger(__name__)
    my_log.debug("debug")
    my_log.info("info")
    my_log.warning("warning")
    my_log.error("error")
