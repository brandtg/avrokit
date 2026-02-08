# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import threading
import logging
import queue
from typing import Iterable

logger = logging.getLogger(__name__)


class BlockingQueueAvroReader:
    """
    Reads
    """

    def __init__(self, data: Iterable[object], daemon: bool = True) -> None:
        self.data = data
        self._reader_queue: queue.Queue = queue.Queue()
        self._reader_thread_done = threading.Event()
        self._reader_thread = threading.Thread(
            target=self._reader_worker, daemon=daemon
        )

    def _reader_worker(self) -> None:
        try:
            for datum in self.data:
                self._reader_queue.put(datum, block=True)
        except Exception as e:
            logger.error("Error in reader thread")
            logger.exception(e)
        finally:
            self._reader_thread_done.set()

    @property
    def queue(self) -> queue.Queue:
        return self._reader_queue

    def empty(self) -> bool:
        return self._reader_queue.empty() and self._reader_thread_done.is_set()

    def start(self) -> None:
        self._reader_thread.start()

    def stop(self) -> None:
        self._reader_thread_done.set()
        self._reader_thread.join()
