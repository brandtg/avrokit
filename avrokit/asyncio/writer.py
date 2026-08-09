# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import logging
import queue
import threading

from avrokit.io.writer import Appendable

logger = logging.getLogger(__name__)


class DeferredAvroWriter:
    """
    Accepts writes and appends asynchronously in a separate thread.
    """

    def __init__(self, writer: Appendable, daemon: bool = True) -> None:
        self.writer = writer
        self._writer_queue: queue.Queue = queue.Queue()
        self._writer_thread_done = threading.Event()
        self._writer_thread = threading.Thread(
            target=self._writer_worker, daemon=daemon
        )

    def _writer_worker(self):
        while not self._writer_thread_done.is_set() or not self._writer_queue.empty():
            try:
                datum = self._writer_queue.get(timeout=1)
                if datum:
                    self.writer.append(datum)
            except queue.Empty:
                continue
            except Exception as e:
                logger.error("Error in writer thread")
                logger.exception(e)

    def start(self) -> None:
        self._writer_thread.start()

    def stop(self) -> None:
        self._writer_thread_done.set()
        self._writer_thread.join()

    def close(self) -> None:
        self.stop()
        # Close the underlying writer if it supports it (e.g. DataFileWriter)
        close_fn = getattr(self.writer, "close", None)
        if close_fn:
            close_fn()

    def __enter__(self) -> "DeferredAvroWriter":
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def append(
        self, datum: object, block: bool = True, timeout: int | None = None
    ) -> None:
        self._writer_queue.put(datum, block=block, timeout=timeout)
