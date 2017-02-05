from scrapper import Google
from datetime import datetime, timedelta, time
from db_io import DataBase, DbBarData, DbBar

def calc_missing_bar_days(file_last_day):
    now = datetime.now()
    today = now.date()

    if now.time() <= time(hour=23):
        print 'the market day is not yet over, we remove today from count...'
        today -= timedelta(days=1)

    print file_last_day, today

    days = (today - file_last_day).days

    curr = file_last_day
    for i in xrange(days):
        curr = curr + timedelta(days=1)
        # print curr, curr.weekday()

        if curr.weekday() in [5, 6]: # saturday, sunday
            days -= 1
            # print days

    # we can add here logic that remove holidays

    return days

def get_google_bars_in_db_format(symbol, days):
    DATE = 0
    CLOSE = 1
    HIGH = 2
    LOW = 3
    OPEN = 4
    VOLUME = 5

    goog = Google()

    assert days <= 15  # we can only downoad up to 15 days of minute chart data
    g_bars = goog.get_bars(symbol, 60, '%dd' % days)

    bars = []
    count = 0
    for bar in g_bars:
        bars.append(DbBar(bar[DATE], bar[OPEN], bar[HIGH], bar[LOW], bar[CLOSE], bar[VOLUME]))

    return bars

def update_minute_bars(symbol):
    print 'updateing:', symbol

    base_folder = r'C:\Users\dkinsbur\Desktop\stocks\db'

    db = DataBase(base_folder)

    if not db.exist(symbol):
        print 'trying to craete new db file for:', symbol
        bars = get_google_bars_in_db_format(symbol, 15)
        if len(bars) > 0:
            db.store(symbol, bars)
        else:
            print 'cannot get symbobars from google'

        return 


    # get last bar in db file
    data = db.load(symbol)
    last = data.bars[-1]

    # get bars from last in file till today
    file_last_day = datetime.fromtimestamp(last.date).date()

    days = calc_missing_bar_days(file_last_day)
    print 'days to download:', days
    if days > 0:
        g_bars = get_google_bars_in_db_format(symbol, days)

        # remove bars that already exist in database
        count = 0
        bars = []
        for bar in g_bars:
            if bar.date <= last.date:
                count += 1
                continue
            bars.append(bar)
        print 'Extra bars downloaded:', count

        # update database with new bars
        db.append(symbol, bars)

def get_symbol_list_from_csv(fname):
    syms = []
    with open(fname) as f:
        for i, line in enumerate(f):
            syms.append(line.strip(',\n'))
    return syms

def get_symbol_list():
    syms = []
    syms.extend(get_symbol_list_from_csv(r"C:\Users\dkinsbur\Desktop\stocks\nyse_list_23_1_17.csv"))
    syms.extend(get_symbol_list_from_csv(r"C:\Users\dkinsbur\Desktop\stocks\nasdaq_list_23_1_17.csv"))
    return syms

if __name__ == '__main__':
    for symbol in get_symbol_list():
        update_minute_bars(symbol)
