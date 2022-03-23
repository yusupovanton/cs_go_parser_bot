import io
import csv
from handlers.imports import *
from handlers.config import *

"""LOGGING"""


class CsvFormatter(logging.Formatter):

    def __init__(self):
        super().__init__()
        self.output = io.StringIO()
        self.writer = csv.writer(self.output, quoting=csv.QUOTE_ALL)

    def format(self, record):
        self.writer.writerow(['MY_MAC', record.asctime, record.levelname, record.msg])
        data = self.output.getvalue()
        self.output.truncate(0)
        self.output.seek(0)
        return data.strip()


logging.basicConfig(filename=f'logs/run_info.csv',
                    filemode='a',
                    level=logging.INFO)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.FileHandler('test.log', 'a', 'utf-8')
frmt = logging.Formatter('%(asctime)s %(message)s')
handler.setFormatter(frmt)
logger.addHandler(handler)

'''REDIS'''

r = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    charset="utf-8",
    decode_responses=True,
    password=REDIS_PASS
)
