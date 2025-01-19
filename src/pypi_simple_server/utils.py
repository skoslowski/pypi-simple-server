import asyncio
from dataclasses import dataclass, field
from pathlib import Path

from anyio import to_thread


@dataclass
class FileMTimeWatcher:
    file: Path
    polling_interval: float = 5
    mtime: float = field(default=0.0, init=False)

    def start(self):
        self.mtime = self.file.stat().st_mtime
        asyncio.create_task(self.run())

    async def run(self):
        while True:
            stat = await to_thread.run_sync(self.file.stat)
            self.mtime = stat.st_mtime
            await asyncio.sleep(self.polling_interval)
