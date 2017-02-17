
from datetime import datetime
from bar import Bar

class Strategy(object):

    TRADE_TIME_START = datetime(1,1,1, 9, 30)
    TRADE_TIME_END = datetime(1,1,1, 16, 00)
    MAX_PRICE = 50
    MIN_PRICE = 3
    PRICE_RANGE = 0.30
    LENGTH = 30

    STATE_SEEK = 0
    STATE_PREPARE = 1
    STATE_POSITION = 2

    def __init__(self, log):
        self.prev_bars = []
        self.hitkansut = False
        self.up = True
        self.rnd = 0
        self.state = self.STATE_SEEK
        self.log = log

        self.prev_bar = Bar(datetime(1,1,1), 0, 0, 0, 0, 0)

    def check_abort_strategy(self, feed, bar):
        if bar.close > self.MAX_PRICE or bar.close < self.MIN_PRICE:
            feed.abort()
            return True

    def in_trading_time(self, bar):
        d = datetime(1,1,1,bar.date.hour, bar.date.minute)
        return self.TRADE_TIME_START <= d <= self.TRADE_TIME_END

    def check_condition(self):
        low = 1000000
        high = 0
        for b in self.prev_bars:
            low = min(low, b.low)
            high = max(high, b.high)

        res = high - low <= self.PRICE_RANGE and (high == round(high) or low == round(low))

        return res, high, low

    def reset_state(self):
        self.state = self.STATE_SEEK
        self.prev_bars = []

    def on_data(self, feed, bar):

        if self.check_abort_strategy(feed, bar):
            return



        if self.state == self.STATE_SEEK:
            in_trade = self.in_trading_time(bar)
            if (not in_trade) or bar.date.day != self.prev_bar.date.day:
                self.reset_state()

            else:
                self.prev_bars.append(bar)

                if len(self.prev_bars) == self.LENGTH:
                    condition_met, high, low = self.check_condition()
                    if condition_met:
                        self.state = self.STATE_PREPARE
                        if high == round(high):
                            self.up = True
                            self.rnd = round(high)
                        else:
                            self.up = False
                            self.rnd = round(low)

                        self.log.write('>>> {} | {} | {} | {} | {}\n'.format(feed.symbol, bar.date, high, low, high-low))

                    self.prev_bars.pop(0)



        elif self.state == self.STATE_PREPARE:
            if not self.in_trading_time(bar) or bar.date.day != self.prev_bar.date.day:
                self.reset_state()
            else:
                self.prev_bars.append(bar)

                condition_met, high, low = self.check_condition()
                if not condition_met:
                    self.log.write('<<< {} | {} | {} | {} | {}\n'.format(feed.symbol, bar.date, high, low, high-low))
                    self.log.write('-------------------------------------------\n')

                    # if self.up and bar.high > self.rnd:
                    #     self.enter_pos(bar)
                    #     self.log.write('BUY: {}\n'.format(min(bar.high, self.rnd+0.03)))
                    #     self.state = self.STATE_POSITION
                    # elif (not self.up) and bar.low < self.rnd:
                    #     self.log.write('SELL: {}\n'.format(max(bar.low, self.rnd-0.03)))
                    #     self.state = self.STATE_POSITION

                    self.state = self.STATE_SEEK

                self.prev_bars.pop(0)


        # elif self.state == self.STATE_POSITION:
        #     if not self.in_trading_time(bar) or bar.date.day != self.prev_bar.date.day:
        #         self.log.write('SHOULD have closed POS!!!\n')
        #         self.close_position(self.prev_bar)
        #         self.reset_state()
        #     else:
        #         if

        self.prev_bar = bar


    def on_end(self, feed):
        print 'end >> ', feed.symbol

    def close_position(self, prev_bar):
        self.log.write('need to close POS at {}'.format(prev_bar))

    def enter_pos(self, bar):
        pass

