import lxml.html
import struct
import os

class DbBar(object):
    def __init__(self, date, open, high, low, close, volume):
        self.date = date
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

class DbBarData(object):
    def __init__(self, ver, period, symbol, bars):
        self.ver = ver
        self.period = period
        self.symbol = symbol
        self.bars = bars

class DataBase(object):
    version = 1
    DAY = 0
    MIN = 1
    # period = MIN

    header = 'BBBBI8s' # struct: ver (uint8), period (uint8) reserved(2 byte), entries(uint32), symbol (str 8 byte)
    entry = 'IffffI' # DATE(unit32),CLOSE(float),HIGH(float),LOW(float),OPEN(float),VOLUME(uint32)

    def __init__(self, folder):
        self.folder = folder

    def db_file(self, symbol):
        return os.path.join(self.folder, symbol + '.bin')

    def exist(self, symbol):
        return os.path.exists(self.db_file(symbol))

    def load(self, symbol):
        db_file = self.db_file(symbol)
        with open(db_file, 'rb') as fl:
            ver, period, _, _, size, sym = struct.unpack(DataBase.header, fl.read(struct.calcsize(DataBase.header)))
            assert os.path.getsize(db_file) == struct.calcsize(DataBase.header)+size*struct.calcsize(DataBase.entry)

            bars = []
            for i in range(size):
                date, Open, high, low, close, volume = struct.unpack(DataBase.entry, fl.read(struct.calcsize(DataBase.entry)))
                Open = round(Open, 4)
                high = round(high, 4)
                low = round(low, 4)
                close = round(close, 4)

                bars.append(DbBar(date, Open, high, low, close, volume))

        return DbBarData(ver, period, symbol, bars)

    def load_last_sample(self, symbol):
        db_file = self.db_file(symbol)
        with open(db_file, 'rb') as fl:
            ver, period, _, _, size, sym = struct.unpack(DataBase.header, fl.read(struct.calcsize(DataBase.header)))
            assert os.path.getsize(db_file) == struct.calcsize(DataBase.header)+size*struct.calcsize(DataBase.entry)
            assert size > 0
            fl.seek(-struct.calcsize(DataBase.entry), 2)

            date, Open, high, low, close, volume = struct.unpack(DataBase.entry, fl.read(struct.calcsize(DataBase.entry)))
            Open = round(Open, 4)
            high = round(high, 4)
            low = round(low, 4)
            close = round(close, 4)

            return DbBar(date, Open, high, low, close, volume)

    def store(self, symbol, bars):
         db_file = self.db_file(symbol)
         with open(db_file, 'wb') as fl:
            fl.write(struct.pack(DataBase.header, DataBase.version, DataBase.MIN, 0, 0, len(bars), symbol))
            for bar in bars:
                fl.write(struct.pack(DataBase.entry, bar.date, bar.close, bar.high, bar.low, bar.open, bar.volume))

    def append(self, symbol, bars):
        fname = self.db_file(symbol)
        with open(fname, 'rb+') as fl:
            fl.seek(0)
            ver, period, _, _, size, sym = struct.unpack(DataBase.header, fl.read(struct.calcsize(DataBase.header)))
            fl.seek(0)
            size += len(bars)
            fl.write(struct.pack(DataBase.header, ver, period, 0, 0, size, sym))
            fl.seek(0, 2)
            for bar in bars:
                 # DATE(unit32),CLOSE(float),HIGH(float),LOW(float),OPEN(float),VOLUME(uint32)
                fl.write(struct.pack(DataBase.entry, bar.date, bar.close, bar.high, bar.low, bar.open, bar.volume))


