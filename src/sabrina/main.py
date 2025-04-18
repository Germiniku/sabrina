from datetime import timedelta
import os
import time
import typer
from loguru import logger
from scheduler import Scheduler
import signal
import asyncio

from sabrina.fetcher.futures import top_long_short_position_ratio
from sabrina.fetcher.kline import KlineWs

app = typer.Typer()


def signal_handler():
    logger.info("receive signal, exit")
    os._exit()


@app.command()
def start():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    loop = asyncio.new_event_loop()
    loop.run_until_complete(run_ws())


@app.command()
def cron():
    """ """
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    scheduler = Scheduler()
    scheduler.cyclic(timedelta(minutes=5), top_long_short_position_ratio)
    while True:
        scheduler.exec_jobs()
        time.sleep(1)


async def run_ws():
    manager = KlineWs()
    await manager.start()
    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    app()
