from datetime import timedelta, datetime
from collections import namedtuple

Bar = namedtuple('Bar', ('date', 'high', 'low', 'open', 'close', 'volume'))

def market_time(date):
    start = datetime(year=date.year, month=date.month, day=date.day, hour=9, minute=30)
    end = datetime(year=date.year, month=date.month, day=date.day, hour=16, minute=00)
    return start, end

def to_bar_object(args):
    DATE_TIME_FORMAT = '%Y/%m/%d-%H:%M'
    DATE_FORMAT = '%Y/%m/%d'
    TIME_SELL_FORMAT = '%H:%M:%S'
    format = DATE_TIME_FORMAT if '-' in args[0] else DATE_FORMAT  # pick date time format according to the bar type (minute/day)

    # bar format: "<DATE-TIME> <HIGH> <LOW> <OPEN> <CLOSE> <VOLUME>"
    return Bar(
        date=datetime.strptime(args[0], format),
        high=float(args[1]),
        low=float(args[2]),
        open=float(args[3]),
        close=float(args[4]),
        volume=int(args[5]))

NEW_YORK_OFFSET = timedelta(hours=-7)
from das import DasChartCollector
from scrapper import Finviz

######### save minute bars to file
def save_to_files(syms):
    from account_info import ACCOUNT_INFO
    TCP_IP = '127.0.0.1'
    TCP_PORT = 9800
    USER = ACCOUNT_INFO['USER']
    PASS = ACCOUNT_INFO['PASSWORD']
    ACCOUNT = ACCOUNT_INFO['ACCOUNT']

    with DasChartCollector(TCP_IP, TCP_PORT, USER, PASS, ACCOUNT) as broker:
        while syms:
            symbols = syms[:50]
            syms = syms[50:]
            s,e = market_time(datetime.today()-timedelta(days=1))
            bars = broker.get_symbols_bars(symbols, s, e)
            for symbol in symbols:
                print symbol, len(bars[symbol])
                prev = datetime.now()
                with open('DB__' + symbol + '.txt', 'w') as f:
                    l = []
                    for x in bars[symbol]:
                        l.append(to_bar_object(x))
                    f.write(str(l))

def research(directory, syms):
    import datetime
    MIN_TREND = 0.5
    MAX_DOWN = 0.15
    for symbol in syms:
        print '='*10, symbol, '='*10
        with open(directory + '\\' + 'DB__' + symbol + '.txt') as f:
            bars = eval(f.read())
            bars.sort(key=lambda x: x.date)
        start = None
        mx = 0
        for bar in bars:
            if start == None:
                start = bar
                mx = bar.high

            elif (bar.low < start.low) or (mx - bar.close > MAX_DOWN):
                if mx - start.low > MIN_TREND and bar.date-start.date >= timedelta(minutes=15) and bar.date.hour >= 10:
                    print '-------------------------------'
                    print start
                    print bar
                    print mx - start.low, bar.date-start.date
                    print '-------------------------------'
                    print
                start = bar
                mx = start.high
            else:
                mx = max(bar.high, mx)


syms = ['A','AA','AAL','ABT','ADM','AES','AIV','AMAT','AME','AN','ATVI','BAC','BAX','BBBY','BBT','BBY','BEN','BF-B','BK','BSX','BWA','CA','CAG','CBG','CCE','CERN','CF','CFG','CHD','CHK','CMS','CNP','COG','COH','CSCO','CSX','CTL','DAL','DHI','DISCA','DISCK','DO','DVN','EBAY','ENDP','ETFC','EXC','F','FAST','FCX','FE','FITB','FLIR','FLS','FOXA','FSLR','FTI','GE','GGP','GLW','GM','GPS','GT','HBAN','HBI','HCP','HIG','HOLX','HPE','HPQ','HRB','HRL','HST','INTC','IPG','IRM','IVZ','JCI','JNPR','JWN','KEY','KIM','KMI','KO','KORS','KR','KSS','L','LEG','LEN','LKQ','LM','LNT','LUK','M','MAS','MAT','MDLZ','MNST','MOS','MRO','MS','MU','MUR','MYL','NAVI','NBL','NEM','NFX','NI','NLSN','NOV','NRG','NTAP','NWL','NWSA','OI','ORCL','PBCT','PBI','PDCO','PEG','PFE','PGR','PHM','PPL','PWR','PYPL','RF','RHI','RIG','RRC','SCHW','SE','SEE','SO','SPLS','STX','SWN','SYF','SYMC','T','TDC','TGNA','TRIP','TXT','UA','UDR','UNM','URBN','VIAB','WFM','WMB','WU','WY','XEL','XL','XRX','YHOO','ZION']
directory = '..'

if __name__ == '__main__':
    # syms = ['SPY', 'QQQ']
    # save_to_files(syms)
    research(directory, syms)
