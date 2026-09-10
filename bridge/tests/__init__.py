"""Test package.

The bridge logs a warning per dead-lettered frame and per reconnect by design. Several tests
deliberately exercise both, so quieten the loggers for the duration — failures still surface, since
unittest reports those itself.
"""

import logging

for name in ("p01.bridge", "p01.bridge.cursor", "p01.bridge.produce"):
    logging.getLogger(name).setLevel(logging.CRITICAL)
