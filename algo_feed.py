from datetime import datetime

class Feed(object):

    def __init__(self):
        self._evt_on_data = []
        self._evt_on_end = []
        self.abrt = False

    def register(self, on_data, on_end):
        if on_data:
            self._evt_on_data.append(on_data)
        if on_end:
            self._evt_on_end.append(on_end)

    def _get_data_iterator(self):
        raise NotImplementedError

    def go(self):
        for data in self._get_data_iterator():
            for cb in self._evt_on_data:
                cb(self, data)

            if self.abrt:
                return

        for cb in self._evt_on_end:
            cb(self)

    def abort(self):
        self.abrt = True

class Bar(object):

    def __init__(self, time, high, low, open, close, volume):
        self.time = time # epoch time
        self.high = high
        self.low = low
        self.open = open
        self.close = close
        self.volume = volume

    def get_time(self):
        return datetime.fromtimestamp(self.time)


class GoogleFileFeed(Feed):
    def __init__(self, fname, symbol):
        super(GoogleFileFeed, self).__init__()
        self.fname = fname
        self.symbol = symbol

    def _get_data_iterator(self):
        return (line.split(',') for line  in self.google_file)

    def go(self):
        with open(self.fname) as self.google_file:
            first_line = self.google_file.readline()  # skip title line
            assert first_line.strip() == 'DATE,CLOSE,HIGH,LOW,OPEN,VOLUME', [first_line]
            super(GoogleFileFeed, self).go()

import bar
class DasFileFeed(Feed):
    def __init__(self, symbol, db_dir):
        super(DasFileFeed, self).__init__()
        self.fname = db_dir + '\\' + 'DB__' + symbol + '.txt'
        self.symbol = symbol

    def go(self):
        with open(self.fname) as self.bar_file:
            first_line = self.bar_file.readline()  # skip title line
            assert first_line.strip() == 'date,high,low,open,close,volume', [first_line]
            super(DasFileFeed, self).go()

    def _get_data_iterator(self):
        args = []
        return (bar.file_bar_to_bar_obj(line) for line in self.bar_file)

