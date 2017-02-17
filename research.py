from datetime import timedelta, datetime
from bar import Bar
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
def save_to_files(syms, start, end):
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
            bars = broker.get_symbols_bars(symbols, start, end)
            for symbol in symbols:
                print symbol, len(bars[symbol])
                prev = datetime.now()
                with open('DB__' + symbol + '.txt', 'w') as f:
                    f.write('date,high,low,open,close,volume\n')
                    l = []
                    for x in bars[symbol]:
                        l.append(to_bar_object(x))
                        # l.sort(key=lambda x: x.date)
                    for bar in l:
                        f.write('{}-{}-{}-{}-{},{},{},{},{},{}\n'.format(
                            bar.date.year,
                            bar.date.month,
                            bar.date.day,
                            bar.date.hour,
                            bar.date.minute,
                            bar.high,
                            bar.low,
                            bar.open,
                            bar.close,
                            bar.volume,
                        ))


def research(directory, syms):
    import datetime
    MIN_TREND = 0.5
    MAX_DOWN = 0.15
    MIN_POSITION_TIME = timedelta(minutes=15)
    POSITION_ENDING_HOUR = 10
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
                if mx - start.low > MIN_TREND and bar.date-start.date >= MIN_POSITION_TIME and bar.date.hour >= POSITION_ENDING_HOUR:
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


# syms = ['A','AA','AAL','ABT','ADM','AES','AIV','AMAT','AME','AN','ATVI','BAC','BAX','BBBY','BBT','BBY','BEN','BF-B','BK','BSX','BWA','CA','CAG','CBG','CCE','CERN','CF','CFG','CHD','CHK','CMS','CNP','COG','COH','CSCO','CSX','CTL','DAL','DHI','DISCA','DISCK','DO','DVN','EBAY','ENDP','ETFC','EXC','F','FAST','FCX','FE','FITB','FLIR','FLS','FOXA','FSLR','FTI','GE','GGP','GLW','GM','GPS','GT','HBAN','HBI','HCP','HIG','HOLX','HPE','HPQ','HRB','HRL','HST','INTC','IPG','IRM','IVZ','JCI','JNPR','JWN','KEY','KIM','KMI','KO','KORS','KR','KSS','L','LEG','LEN','LKQ','LM','LNT','LUK','M','MAS','MAT','MDLZ','MNST','MOS','MRO','MS','MU','MUR','MYL','NAVI','NBL','NEM','NFX','NI','NLSN','NOV','NRG','NTAP','NWL','NWSA','OI','ORCL','PBCT','PBI','PDCO','PEG','PFE','PGR','PHM','PPL','PWR','PYPL','RF','RHI','RIG','RRC','SCHW','SE','SEE','SO','SPLS','STX','SWN','SYF','SYMC','T','TDC','TGNA','TRIP','TXT','UA','UDR','UNM','URBN','VIAB','WFM','WMB','WU','WY','XEL','XL','XRX','YHOO','ZION']
directory = '..'



def load_bars(symbol):
    bars = []
    with open('DB__' + symbol + '.txt') as f:
        for i,line in enumerate(f):
            if i == 0:
                continue
            args = line.split(',')
            b = Bar(
                date=datetime(*[int(x) for x in args[0].split('-')]),
                high=args[1],
                low=args[2],
                open=args[3],
                close=args[4],
                volume=args[5]
            )
            bars.append(b)
            # bars.sort()
            if i > 500:
                break
    return bars



def find_hitkansut(symbol, folder, log):
    from algo_feed import DasFileFeed
    from strategy import Strategy
    s = Strategy(log)

    feed = DasFileFeed(symbol,folder)


    feed.register(s.on_data, s.on_end)

    feed.go()

if __name__ == '__main__':
    # while symbols:
    #     syms = symbols[:5]
    #     symbols = symbols[5:]
    #     start = datetime(day=1, month=1, year=2017)
    #     end = datetime(day=15, month=2, year=2017, hour=16, minute=30)
    #     # end = datetime(day=3, month=1, year=2017, hour=16, minute=30)
    #     print start, end
    #     save_to_files(syms, start, end)
    # research(directory, syms)
    folder = r'C:\Users\dkinsbur\Documents\Work\pythonProjects\AlgoTrade\DataBase'
    import os
    with open('log.log','w') as log:
        for fnam in os.listdir(folder):
            if fnam.startswith('DB__'):
                find_hitkansut(fnam[4:-4], folder, log)
