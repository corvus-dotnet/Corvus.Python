"""Shared timeouts for the HTTP requests made with ``requests`` across corvus_python.

``requests`` waits forever when it isn't given a timeout, so a connection that stalls without closing would block
the calling thread indefinitely. Every request in this package therefore passes one of these timeouts. When a timeout
fires, ``requests`` raises ``requests.Timeout`` (a subclass of ``requests.ConnectionError``), which is left to
propagate so that callers can decide whether to retry.

A timeout is either a single number of seconds, used for both the connect and read timeouts, or a
``(connect, read)`` tuple. The read timeout is the longest time to wait between bytes from the server, not a limit on
the whole request.
"""

from typing import Union

HttpTimeout = Union[float, tuple[float, float]]

DEFAULT_HTTP_TIMEOUT: HttpTimeout = (10, 60)
"""Default ``(connect, read)`` timeout, in seconds, for HTTP requests."""

DEFAULT_UPLOAD_HTTP_TIMEOUT: HttpTimeout = (10, 300)
"""Default ``(connect, read)`` timeout, in seconds, for file uploads, which can take the server longer to answer."""
