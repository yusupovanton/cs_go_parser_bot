from main import *
from handlers import *


async def periodic():

    print('periodic')
    await asyncio.sleep(1)


def stop():
    task.cancel()


loop = asyncio.get_event_loop()

task = loop.create_task(periodic())

try:
    loop.run_until_complete(task)
except asyncio.CancelledError:
    pass

