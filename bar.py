from collections import namedtuple
from datetime import datetime
Bar = namedtuple('Bar', ('date', 'high', 'low', 'open', 'close', 'volume'))

# date,high,low,open,close,volume
# 2017-1-3-8-0,28.3,28.3,28.3,28.3,200
def file_bar_to_bar_obj(file_bar):
    args = file_bar.split(',')
    return Bar(
            date=datetime(*[int(x) for x in args[0].split('-')]),
            high=float(args[1]),
            low=float(args[2]),
            open=float(args[3]),
            close=float(args[4]),
            volume=int(args[5]))
