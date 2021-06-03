import io
import asyncio


class NopProtocol:
    def __init__(self, messages=()):
        self.messages = messages
        self.i = 0

    def write(self, _):
        return

    def flush(self):
        return

    def read(self, _):
        if self.i < len(self.messages):
            self.i += 1
            return self.messages[self.i - 1]
        return b""


class NopProcess:
    def __init__(
        self,
        final_stdout: bytes = None,
        final_stderr: bytes = None,
        stdout_messages=(),
    ):
        self.final_stdout = final_stdout
        self.final_stderr = final_stderr
        self.stdout_messages = stdout_messages
        self.stdout_i = 0
        self.stdin = NopProtocol()
        self.stdout = io.BytesIO(b"".join(stdout_messages))

    def communicate(self, _=None):
        return self.final_stdout, self.final_stderr


class AsyncNopProtocol:
    def __init__(self, messages=(), message_delay=0):
        self.messages = messages
        self.message_delay = message_delay
        self.i = 0

    def write(self, _):
        return

    async def drain(self):
        return

    async def read(self, _):
        if self.message_delay:
            await asyncio.sleep(self.message_delay)
        if self.i < len(self.messages):
            self.i += 1
            return self.messages[self.i - 1]
        return b""


class AsyncNopProcess:
    def __init__(
        self,
        final_stdout: bytes = None,
        final_stderr: bytes = None,
        stdout_messages=(),
        message_delay=0,
        final_delay=0,
    ):
        self.final_stdout = final_stdout
        self.final_stderr = final_stderr
        self.final_delay = final_delay
        self.stdout_messages = stdout_messages
        self.stdout_i = 0
        self.stdin = AsyncNopProtocol()
        self.stdout = AsyncNopProtocol(
            messages=stdout_messages, message_delay=message_delay
        )

    async def wait(self):
        return

    def terminate(self):
        return

    def kill(self):
        return

    async def _feed_stdin(self, _):
        return

    @staticmethod
    async def _noop():
        return None

    async def _read_stream(self, _):
        if self.stdout_i < len(self.stdout_messages):
            message = self.stdout_messages[self.stdout_i]
        else:
            message = b""
        self.stdout_i += 1
        return message

    async def communicate(self, _=None):
        if self.final_delay:
            await asyncio.sleep(self.final_delay)
        return self.final_stdout, self.final_stderr
