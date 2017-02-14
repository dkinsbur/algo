from datetime import datetime, timedelta
import time
import threading
import socket
import logging

LOGGER_FORMAT_STRING='[%(name)-20s: %(levelname)-8s: %(asctime)-24s] %(message)s'

LEVEL1 = 'Lv1'
LEVEL2 = 'Lv2'
TIME_AND_SELL = 'tms'
DAY_CHART = 'DAYCHART'
MIN_CHART = 'MINCHART'
QUOTE_TYPES = (LEVEL1, LEVEL2, TIME_AND_SELL, DAY_CHART, MIN_CHART)


class DasBroker(object):

    def __init__(self, ip, port, user, password, account, log_level=logging.ERROR):
        self.quote_reg = {}
        self.connected = False
        self.kill_th = True
        self.listener_alive = True
        self.callbacks = []

        # logger
        self.logger = logging.getLogger('DasBroker')
        hdlr = logging.StreamHandler()
        hdlr.setFormatter(logging.Formatter(LOGGER_FORMAT_STRING))
        self.logger.addHandler(hdlr)
        self.logger.setLevel(log_level)

        self.listener_th = threading.Thread(target=DasBroker._listener, args=(self,))
        self.listener_th.daemon = True

        self.ip = ip
        self.port = port

        self.socket = socket.socket()

        self.account = account
        self.user = user
        self.password = password

    def __enter__(self):

        self.logger.debug('connect()')
        assert not self.connected

        self.kill_th = False
        self.socket.connect((self.ip, self.port))
        self.listener_th.start()

        self.send('LOGIN {USER} {PASSWORD} {ACCOUNT}'.format(USER=self.user, PASSWORD=self.password, ACCOUNT=self.account))
        self.connected = True

        del self.password

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.debug('disconnect()')
        assert self.connected

        # set kill_th to signal thread to exit while loop
        self.kill_th = True

        # close socket connection to DAS
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()

        # wait for threads to terminate
        self.listener_th.join()

        # cleanup state
        self.connected = False

    def _listener(self):
        left_over_buf = ''
        while not self.kill_th:
            buf = self.socket.recv(4096)
            left_over_buf = self._handle_input_message(left_over_buf+buf)
            if left_over_buf:
                self.logger.debug('left over: "%s"', left_over_buf)

    def _handle_input_message(self, msg):
        left_over = ''
        if msg and msg[-2:] != '\r\n':
            left_over_start = msg.rfind('\r\n')
            if left_over_start > -1:
                left_over = msg[left_over_start + 2:]
                msg = msg[:left_over_start]

        for line in msg.splitlines():
            assert '\r' not in line
            self.logger.info('>>> %s', line)
            if not line.startswith('$'):
                print '>>>>>>>>>>', line
            for cb in self.callbacks:
                cb(line)

        return left_over

    def _handle_output_message(self, msg):

        # print all messages besides the login message which contains the password
        if not msg.startswith('LOGIN '):
            self.logger.info('<<< %s', msg)

        self.socket.send(str(msg) + '\r\n')

    def send(self, msg):
        self._handle_output_message(msg)

    def register(self, callback):
        self.logger.debug('register({})'.format(callback))
        self.callbacks.append(callback)

    def unregister(self, callback):
        self.logger.debug('unregister({})'.format(callback))
        self.callbacks.remove(callback)


class DasChartCollector(DasBroker):

    # NY_OFFSET = timedelta(hours=-7)

    def __init__(self, ip, port, user, password, account, log_level=logging.ERROR):
        DasBroker.__init__(self, ip, port, user, password, account, log_level=logging.ERROR)
        self.register(self._msg_handler)
        self.stocks = {}
        self.done_threshold = timedelta(milliseconds=1000)
        self.timeout = timedelta(seconds=10)
        self.terminate = False

    def _msg_handler(self, msg):
        try:
            if msg.startswith('$Bar'):
                self._handle_bar(msg)
            elif msg[0] in '#%' or msg == 'LOGIN SUCCESSED':
                print msg
                if msg.startswith("#Can't"):
                    raise
            else:
                raise
        except:
            self.terminate = True

    def _handle_bar(self, msg):
        data = msg.split()

        self.stocks[data[1]]['bars'].append(data[2:])

        now = datetime.now()
        delta = now - self.stocks[data[1]]['last_timestamp']

        self.stocks[data[1]]['last_timestamp'] = now
        self.stocks[data[1]]['max'] = max(self.stocks[data[1]]['max'], delta)

    def _datetime_to_string(self, dt_tm, quote_type):
        dt_tm_str = '%04d/%02d/%02d' % (dt_tm.year, dt_tm.month, dt_tm.day)
        if quote_type== MIN_CHART:
            dt_tm_str += '-%02d:%02d' % (dt_tm.hour, dt_tm.minute)

        return dt_tm_str

    def subscribe_min_bar(self, symbol, start, end):

        start = self._datetime_to_string(start, MIN_CHART)
        end = self._datetime_to_string(end, MIN_CHART)
        self.stocks[symbol] = {'bars': [], 'last_timestamp': datetime.now() + self.timeout, 'max': timedelta(), 'done': False}
        self.send('SB {} {} {} {}'.format(symbol, MIN_CHART, start, end))


    def is_min_bar_done(self, symbol=None):
        if symbol is not None:
            return self._is_min_bar_done(symbol)
        else:
            for sym in self.stocks:
                if not self._is_min_bar_done(sym):
                    return False
            return True

    def _is_min_bar_done(self, symbol):
        if self.stocks[symbol]['done']:
            return True

        now = datetime.now()
        last = self.stocks[symbol]['last_timestamp']
        delta = now - last
        if delta > self.done_threshold:
            self._unsubscribe_min_bar(symbol)
            self.stocks[symbol]['done'] = True
            return True
        return False

    def _unsubscribe_min_bar(self, symbol):
        self.send('UNSB {} {}'.format(symbol, MIN_CHART))

    def unsubscribe_all(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.unsubscribe_all()
        self.unregister(self._msg_handler)
        time.sleep(0.01)
        DasBroker.__exit__(self, exc_type, exc_val, exc_tb)

    def get_symbols_bars(self, symbols, start, end):
        bars = {}
        for symbol in symbols:
            bars[symbol] = list()

        print bars
        s = start
        e = s + min(timedelta(days=7), end-s)
        while s < end:
            print s, e
            curr = datetime.now()
            for symbol in symbols:
                self.subscribe_min_bar(symbol, s, e)
            while not self.is_min_bar_done():
                print '.',
                time.sleep(0.1)
                if self.terminate:
                    self.terminate = False
                    return None

            print ''
            s = e
            e = s + min(timedelta(days=7), end-s)

            for symbol in symbols:
                bars[symbol].extend(self.stocks[symbol]['bars'])

        return bars

