"""Test package. The sink logs a warning per skipped row by design, and several tests exercise that
path deliberately; quieten it so failures stand out."""

import logging

for name in ("p01.sink", "p01.sink.writer"):
    logging.getLogger(name).setLevel(logging.CRITICAL)
