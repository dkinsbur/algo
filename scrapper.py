import urllib2
import lxml.html

class Finviz(object):
    def get_screener(self, filter):
        symbols = []

        xml_search_symbols = ".//a[@class='screener-link-primary']"
        xml_search_table = ".//div[@id='screener-content']"
        url = 'http://finviz.com/screener.ashx?v=111&f={}'.format(filter)
        xml_search_last_table = ".//a[@class='screener-pages']"

        response = urllib2.urlopen(url)
        html = response.read()
        root = lxml.html.fromstring(html)
        table = root.find(xml_search_table)

        for x in table.iterfind(xml_search_symbols):
            symbols.append(x.text)

        x = table.findall(xml_search_last_table)
        symbol_max = 0
        if x:
            symbol_max = int(x[-1].attrib['href'].split('=')[-1])

        symbol_count = 21
        while symbol_count <= symbol_max:
            print symbol_count, symbol_max
            url_x = 'http://finviz.com/screener.ashx?v=111&f={}&r={}'.format(filter, symbol_count)
            response = urllib2.urlopen(url_x)
            html = response.read()
            root = lxml.html.fromstring(html)
            table = root.find(xml_search_table)

            for x in table.iterfind(xml_search_symbols):
                symbols.append(x.text)

            symbol_count += 20

        return symbols

    def get_stock_info(self, symbol):

        info = {}

        response = urllib2.urlopen('http://finviz.com/quote.ashx?t={symbol}'.format(symbol=symbol))
        html = response.read()
        root = lxml.html.fromstring(html)

        table = root.find(".//table[@class='fullview-title']")
        info['Company'] = table[1][0][0][0].text
        info['Sector'] = table[2][0][0].text
        info['Industry'] = table[2][0][1].text
        info['Country'] = table[2][0][2].text

        table = root.find(".//table[@class='snapshot-table2']")

        for row in table.iterfind('./tr'):
            cells = row.findall('./td')
            for i in range(0, len(cells), 2):
                key = cells[i].text
                value = cells[i+1]
                while len(value) > 0:
                    value = value[0]
                value = value.text
                info[key] = value

        return info


# PERIOD_MINUTE = 'm'
# PERIOD_DAY = 'd'
# PERIOD_YEAR = 'Y'
#
# INTERVAL_MINUTE = 60
# INTERVAL_DAY = 86400
#
# INTERVAL = INTERVAL_MINUTE
# PERIOD = (1, PERIOD_DAY)
# SYMBOL = 'MOMO'

# COLUMNS=DATE,CLOSE,HIGH,LOW,OPEN,VOLUME

class Google(object):
    def get_bars(self, symbol, interval, period):
        URL_TEMPLATE = 'https://www.google.com/finance/getprices?i={interval}&p={period}&f=d,o,h,l,c,v&q={symbol}'
        url = URL_TEMPLATE.format(interval=interval, period=period, symbol=symbol)
        print url
        fl = urllib2.urlopen(url)
        bars = []
        for i, line in enumerate(fl):
            if line[0] not in 'a1234567890':
                continue
            args = line.strip().split(',')
            data = [float(x) for x in args[1:-1]]
            data.append(int(args[-1]))
            if args[0].startswith('a'):
                curr_time = int(args[0][1:])
                data.insert(0, curr_time)
            else:
                data.insert(0, curr_time + int(args[0])*interval)

            bars.append(data)

            # print i+1, data
        return bars

