# -*- coding: utf-8 -*-
"""
lories.connectors.cameras.stream
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""

from __future__ import annotations

from multiprocessing.shared_memory import SharedMemory
from multiprocessing.synchronize import Event as EventType
from threading import Thread
from time import sleep, time

from lories.connectors.cameras._core import _CameraConnector as CameraConnector
from lories.connectors.errors import ConnectorError
from lories.connectors.tasks.process import ProcessContext
from lories.core import Configurations, Configurator, ResourceUnavailableError
from lories.data import Channels


class CameraStream(Configurator, Thread):
    TYPE: str = "stream"

    __context: ProcessContext
    __channels: Channels
    __connector: CameraConnector

    __trigger: EventType
    __interrupt: EventType
    __failed: bool

    _memory: SharedMemory
    _buffer: memoryview

    # noinspection PyProtectedMember
    def __init__(
        self,
        connector: CameraConnector,
        channels: Channels,
        configs: Configurations,
    ):
        super().__init__(configs, name=f"{connector.id}.stream", target=self.__stream, args=(channels,), daemon=True)
        _process = ProcessContext(configs=configs)
        _manager = _process._context.Manager()
        self.__context = _process
        self.__channels = channels
        self.__connector = connector
        self.__interrupt = _manager.Event()
        self.__trigger = _manager.Event()
        self.__failed = False
        # Frames the subprocess has written to shared memory. Compared against
        # the main thread's local consumed counter to surface the drop rate.
        self.__produced = _manager.Value("i", 0)

        self._memory = SharedMemory(create=True, size=CameraConnector.SIZE + 4)
        self._buffer = self._memory.buf

    def configure(self, configs: Configurations) -> None:
        super().configure(configs)
        # Forward the real, unwrapped Configurations (not the DEBUG warn-proxy) to the
        # nested ProcessContext — its _assert_configs requires an actual _Configurations.
        self.__context.configure(self.configs)

    def is_failed(self) -> bool:
        return self.__failed

    # noinspection PyProtectedMember
    def start(self):
        _camera = self.__connector.duplicate(context=self.__context.connectors)
        _channels = self.__channels.duplicate(context=self.__context)

        self.__context.activate()
        future = self.__context._submit(
            _stream,
            _camera,
            _channels,
            self.__trigger,
            self.__interrupt,
            self._memory.name,
            self.__produced,
        )
        # Worker exceptions otherwise vanish silently — the Future is discarded.
        future.add_done_callback(self._on_stream_done)
        super().start()

    def _on_stream_done(self, future) -> None:
        exc = future.exception()
        if exc is not None:
            self.__failed = True
            self.__interrupt.set()
            self._logger.error(
                f"stream {self.__connector.id} subprocess died: {exc!r}",
                exc_info=exc,
            )

    def stop(self) -> None:
        self.__interrupt.set()
        # Join the consumer thread before tearing down the manager — its loop
        # calls trigger.wait() through the Manager IPC, which raises EOFError
        # if the manager process dies first.
        if self.is_alive():
            self.join()
        self.__context.deactivate()

        # Release the memoryview before closing the shared memory segment,
        # otherwise SharedMemory.close() raises "BufferError: memoryview has
        # 1 exported buffer" during GC.
        self._buffer.release()
        self._memory.close()
        self._memory.unlink()

    def __stream(self, channels: Channels) -> None:
        consumed = 0
        last_report = time()
        last_produced = 0
        last_consumed = 0
        try:
            while not self.__interrupt.is_set():
                self.__trigger.wait(1)

                if self.__interrupt.is_set():
                    break
                if not self.__trigger.is_set():
                    continue
                length = int.from_bytes(self._buffer[0:4], "little")
                data = bytes(self._buffer[4 : 4 + length])
                for channel in channels:
                    channel.value = data
                self.__trigger.clear()
                consumed += 1

                now = time()
                if now - last_report >= 5.0:
                    produced = int(self.__produced.value)
                    dp = produced - last_produced
                    dc = consumed - last_consumed
                    drop = (dp - dc) / dp if dp > 0 else 0.0
                    self._logger.debug(
                        f"stream {self.__connector.id}: "
                        f"produced={dp / (now - last_report):.1f}/s "
                        f"consumed={dc / (now - last_report):.1f}/s "
                        f"dropped={drop:.0%}"
                    )
                    last_report = now
                    last_produced = produced
                    last_consumed = consumed
        except (EOFError, BrokenPipeError, ConnectionResetError):
            # Manager IPC torn down — treat as shutdown signal.
            return


def _stream(
    camera: CameraConnector,
    channels: Channels,
    trigger: EventType,
    interrupt: EventType,
    memory_name: str,
    produced,
    fps: int = 30,
) -> None:
    camera.connect(channels)

    # All streaming channels on a camera share the same RTSP capture; we
    # read one frame per cycle and the main-process __stream loop fans it
    # out to every channel. Pick any channel as the read source.
    source = next(iter(channels))

    memory = SharedMemory(name=memory_name)
    buffer = memory.buf
    try:
        while not interrupt.is_set():
            now = time()
            if not camera.is_connected():
                interrupt.set()
                return

            payload = camera.read_frame(source)
            length = len(payload)
            buffer[0:4] = length.to_bytes(4, "little")
            buffer[4 : 4 + length] = payload
            trigger.set()
            produced.value += 1

            sleep_seconds = (1 / fps) - (time() - now)
            if sleep_seconds > 0:
                sleep(sleep_seconds)
    finally:
        # Match the main-process release: drop the memoryview before
        # closing the shared memory mapping in this subprocess.
        buffer.release()
        memory.close()
        camera.disconnect()


class CameraStreamError(ConnectorError):
    """
    Raise if an error occurred accessing the stream.

    """


class CameraStreamUnavailableError(ResourceUnavailableError):
    """
    Raise if an accessed stream can not be found.

    """
