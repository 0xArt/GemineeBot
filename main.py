import time
import json
import csv
import linecache
import numpy as np
from scipy.stats import norm
import talib
import APIcalls
from APIcalls import Gemini
from TradeAlgo import TradeAlgo
import pandas as pd

Algo = TradeAlgo()
CC = APIcalls.CryptoCompare()

Gemcon = Gemini()

HEADER = [
    'TIME', 'TIMESTAMP', 'ethbtc', 'OPEN', 'CLOSE', 'HIGH', 'LOW', 'VFROM', 'VTO', 'VWAP',
    'WILLIAMS %R 14 Hour', 'WILLIAMS %R 14 Min', 'EMA 5', 'EMA 10',
    'DEMA 5min', 'DEMA 10min', 'MACD', 'Signal Line', 'Signal Line Slope', 'RSI', 'ULTIMATE OSCILLATOR',
    'ADX', 'STOCH FAST K', 'STOCH FAST D', 'AROON', 'DI +', 'DI -', 'ETH', 'BTC', 'e2b signal', 'vwap cross',
    'EMA 5 3min', 'EMA 10 3min']


def vwap(size):
    data = Gemcon.pastOrders()
    total_vol = 0
    weighted_price = 0
    hist_list = json.loads(data)
    for i in range(1, size):
        hist_trade = hist_list[i]
        vol = float(hist_trade['amount'])
        total_vol = vol + total_vol
        price = float(hist_trade['price'])
        weighted_price = price * vol + weighted_price
    return np.round((weighted_price / total_vol), 5)


def geminiLastPrice():
    tickers = Gemcon.ticker('ethbtc')
    d = json.loads(tickers)
    value = d['last']
    value = float(value)
    return value


def minuteData(exchange, coin, base, interval, limit):
    mClose = []
    mOpen = []
    mVolTo = []
    mvolfrom = []
    mHigh = []
    mLow = []
    data = (CC.minuteHist(exchange, coin, base, interval, limit)).json()
    df = pd.DataFrame(index=[0])
    df['time'] = data['TimeTo']
    for i in range(0, len(data['Data'])):
        mClose.append(float((data['Data'][i]['close'])))
        mOpen.append(float((data['Data'][i]['open'])))
        mVolTo.append(float((data['Data'][i]['volumeto'])))
        mvolfrom.append(float((data['Data'][i]['volumefrom'])))
        mHigh.append(float((data['Data'][i]['high'])))
        mLow.append(float((data['Data'][i]['low'])))
    mOpen = np.asarray(mOpen)
    mClose = np.asarray(mClose)
    mHigh = np.asarray(mHigh)
    mLow = np.asarray(mLow)
    mVolTo = np.asarray(mVolTo)
    mvorlfrom = np.asarray(mvolfrom)
    df['dema5'] = talib.DEMA(mClose, 5)[-1]
    df['dema10'] = talib.DEMA(mClose, 10)[-1]
    df['ema5'] = talib.EMA(mClose, 5)[-1]
    df['ema10'] = talib.EMA(mClose, 10)[-1]
    df['macd sl'] = talib.MACD(mClose, 26, 10, 9)[0][-1]
    df['rsi'] = talib.RSI(mClose)[-1]
    df['uosc'] = talib.ULTOSC(mHigh, mLow, mClose)[-1]
    df['adx'] = talib.ADX(mHigh, mLow, mClose)[-1]
    df['w14'] = talib.WILLR(mHigh, mLow, mClose)[-1]
    df['aroon'] = talib.AROON(mHigh, mLow)[0][-1]
    df['di+'] = talib.PLUS_DI(mHigh, mLow, mClose)[-1]
    df['di-'] = talib.MINUS_DI(mHigh, mLow, mClose)[-1]
    return df


def hourData(exchange, coin, base, interval, limit):
    hclose = []
    hhigh = []
    hlow = []
    data = (CC.hourHist(exchange, coin, base, interval, limit)).json()
    df = pd.DataFrame(index=[0])
    df['time'] = data['TimeTo']
    for i in range(0, len(data['Data'])):
        # logger(fname,data['Data'][i])
        hclose.append(float((data['Data'][i]['close'])))
        hhigh.append(float((data['Data'][i]['high'])))
        hlow.append(float((data['Data'][i]['low'])))
    hclose = np.asarray(hclose)
    hhigh = np.asarray(hhigh)
    hlow = np.asarray(hlow)
    df['w14'] = talib.WILLR(hhigh, hlow, hclose)[-1]
    return df


def roundDown(n, d):
    d = int('1' + ('0' * d))
    return np.floor(n * d) / d


def logger(fname, text, printer=True):
    if (loggingEnable == 'true'):
        f = open(fname, 'a')
        f.write(str(text))
        f.write('\n')
        f.flush()
        f.close()
    if (printer == True):
        print(text)


def restoreRecord(target, record):
    num_lines = sum(1 for line in open('records'))
    for i in range(7, num_lines + 1):
        temp = str(linecache.getline('records', i)).rstrip("\n\r")
        if (temp == target):
            for j in range(i + 1, num_lines + 1):
                temp = str(linecache.getline('records', j)).rstrip("\n\r")
                if (temp != ''):
                    temp = np.fromstring(temp, dtype=float, sep=',')
                    if (len(temp) == 8):
                        record = np.vstack([record, temp])
                    else:
                        break
                else:
                    break
            return record

def addRow(df, data):
    df.loc[-1] = data  # adding a row
    df.index = df.index + 1  # shifting index
    df = df.sort_index()  # sorting by index
    df = df[::-1].reset_index(drop=True)
    return df

def e2bBearTradeExecute(fname, btcOffset, ethOffset, ethReserved, ethbtc):
    global fee
    global e2bRecordBear
    eth = 0

    # check and update balance
    try:
        balance = Gemcon.balances()
        if (balance.status_code == 200):
            balance = balance.json()
            # logger(fname,balance)
            for i in range(0, len(balance)):
                if (balance[i]['currency'] == 'ETH'):
                    eth = float(balance[i]['available']) + ethOffset
                    eth = roundDown(eth, 6)
    except Exception as e:
        logger(fname, 'balance request try fail')
        logger(fname, e)

    # if we have enough eth
    if (eth - ethReserved > 0.001):
        market = json.loads(Gemcon.book('ethbtc'))
        # iteratively scan order book
        for i in range(0, len(market['bids'])):
            bid_rate = np.round(float(market['bids'][i]['price']), 5)
            bid_amount = roundDown(float(market['bids'][i]['amount']), 6)
            # if order book is close to our target
            if (bid_rate / ethbtc[-1] > 0.99):
                # if bid amount is more than the amount of eth available, sell all eth
                if (eth - ethReserved < bid_amount):
                    # execute trade
                    logger(fname, 'selling all eth')
                    amount = roundDown((eth - ethReserved), 6)
                    order = Gemcon.newOrder(format(amount, '.6f'), format(bid_rate, '.5f'), 'sell', None, 'ethbtc')
                    if (order.status_code == 200):
                        order = order.json()
                        temp = ['False', 0, time.time(), float(order['original_amount']), float(order['price']),
                                         int(order['order_id']), 'False']
                        # [confirmation 0, reserved btc 1, time 2, amount 3, rate 4, id 5, delete 6]
                        e2bRecordBear = addRow(e2bRecordBear, temp)
                    else:
                        logger(fname, 'e2b bear order failed')
                        logger(fname, order)
                # if bid amount is less than the available eth, sell some eth
                else:
                    logger(fname, 'selling some eth')
                    order = Gemcon.newOrder(format(bid_amount, '.6f'), format(bid_rate, '.5f'), 'sell', None, 'ethbtc')
                    if (order.status_code == 200):
                        order = order.json()
                        temp = ['False', 0, time.time(), float(order['original_amount']), float(order['price']),
                                         int(order['order_id']), 0, 'False']
                        # [confirmation 0, reserved btc 1, time 2, amount 3, rate 4, id 5, delete 6]
                        e2bRecordBear = addRow(e2bRecordBear, temp)
                    else:
                        logger(fname, 'e2b bear order failed')
                        logger(fname, order)
            else:
                logger(fname, 'order book not close enough')
                break
    else:
        logger(fname, 'not enough eth')


def e2bConfirmCancelOrders(fname, timeLimit):
    global e2bRecordBear
    global fee

    for i in range(0, len(e2bRecordBear)):
        # begin checking all unconfirmed orders
        if (e2bRecordBear["Confirmation"][i] == 'False'):
            temp = Gemcon.orderStatus(e2bRecordBear['ID'][i])
            if (temp.status_code == 200):
                temp = temp.json()
                # if order was fully unfilled and canceled, mark it for deletion from record
                if (temp['is_live'] == False and temp['is_cancelled'] == True and temp['executed_amount'] == '0'):
                    e2bRecordBear['Delete'][i] = 'True'
                # if order has been filled, update the record
                if (temp['is_live'] == False and temp['is_cancelled'] == False):
                    e2bRecordBear["Confirmation"][i] = 'True'
                    e2bRecordBear["Rate"][i] = float(temp['avg_execution_price'])
                    e2bRecordBear["Reserved BTC"][i] = e2bRecordBear['Amount'][i] * e2bRecordBear['Rate'][i] * (1 - fee)
                    logger(fname, 'filled e2b bear trade has been confirmed')
                # if order is still live and not marked for cancellation begin more checks on order
                if (temp['is_live'] == True and temp['is_cancelled'] == False):
                    # check to see if order has been unfilled for too long
                    if (time.time() - int(temp['timestamp']) > timeLimit):
                        # cancel orders that have been unfilled for too long
                        if (Gemcon.cancelOrder(e2bRecordBear['ID'][i]).status_code == 200):
                            temp = Gemcon.orderStatus(e2bRecordBear['ID'][i])
                            if (temp.status_code == 200):
                                temp = temp.json()
                                # if order was completely unfilled, just mark it for deletion from record
                                if (float(temp['executed_amount']) == 0):
                                    e2bRecordBear['Delete'][i] = 'True'
                                    logger(fname, 'canceled fully unfilled order')
                                # if order was partially filled, update the record
                                else:
                                    e2bRecordBear['Amount'][i] = float(temp['executed_amount'])
                                    e2bRecordBear['Confirmed'][i] = 'True'
                                    e2bRecordBear["Reserved BTC"][i] = e2bRecordBear['Amount'][i] * e2bRecordBear['Rate'][i] * (1 - fee)
                                    logger(fname, 'canceled partially filled e2b bear order')
                            else:
                                e2bRecordBear[i][0] = 2
                                logger(fname, 'e2b bear order status (2nd round) failed')
                        # if canceling timed out order fails, reserve full amount of btc just in case
                        else:
                            logger(fname, 'canceling timed out b2e bear order failed')
                            e2bRecordBear["Reserved BTC"][i] = e2bRecordBear['Amount'][i] * e2bRecordBear['Rate'][i] * (1 - fee)
            else:
                # if checking order status fails do nothing
                logger(fname, 'e2b bear order status failed')

            """
            if (e2bRecordBear[i][0] == 2):
                temp = Gemcon.orderStatus(e2bRecordBear[i][5])
                if (temp.status_code == 200):
                    temp = temp.json()
                    if (float(temp['executed_amount']) == 0):
                        e2bRecordBear[i][6] = 1
                        logger(fname, 'canceled bammer order')
                    else:
                        e2bRecordBear[i][3] = float(temp['executed_amount'])
                        e2bRecordBear[i][0] = 1
                        e2bRecordBear[i][1] = e2bRecordBear[i][3] * e2bRecordBear[i][4] * (1 - fee)
                        logger(fname, 'canceled partially filled e2b bear order')
                else:
                    logger(fname, 'e2b bear order status failed')
            """

    e2bRecordBear = e2bRecordBear[e2bRecordBear.Delete != 'True']


def b2eBearTradeExecute(fname, ethbtc, minuteResults, tradeTimeArray):
    global e2bRecordBear
    global b2eRecordBear

    for i in range(0, len(e2bRecordBear)):
        b2e = Algo.btc2eth_signal_with_growth_bear(fname, ethbtc, e2bRecordBear[i][3],
                                                   e2bRecordBear[i][4], time.time() - e2bRecordBear[i][2],
                                                   minuteResults, tradeTimeArray)
        if (e2bRecordBear[i][0] == 1 and b2e[0] == 1):
            logger(fname, 'btc 2 eth bear signal')
            # swap back to eth
            order = Gemcon.newOrder(format(e2bRecordBear[i][3], '.6f'), format(ethbtc[-1], '.5f'), 'buy', None,
                                    'ethbtc')
            if (order.status_code == 200):
                order = order.json()
                logger(fname, order)
                temp = np.array(
                    [0, b2e[1], time.time(), float(order['original_amount']), float(order['price']),
                     int(order['order_id']), 0,
                     e2bRecordBear[i][2]])
                # [confirmation 0,  rn 1, time 2, amount 3, rate 4, id 5, delete 6,  pair time 7]
                b2eRecordBear = np.vstack((b2eRecordBear, temp))
                e2bRecordBear[i][6] = 1
                total = float(e2bRecordBear[i][3]) * float(ethbtc[-1]) * (1 - fee)
            else:
                logger(fname, 'b2e bear trade failed')


def b2eConfirmCancelUpdateOrders(fname):
    global b2eRecordBear
    global graveyardRecordBear

    for i in range(0, len(b2eRecordBear)):
        if (int(b2eRecordBear[i][5]) != 0 and int(b2eRecordBear[i][5]) != 2):
            temp = Gemcon.orderStatus(b2eRecordBear[i][5])
            if (temp.status_code == 200):
                temp = temp.json()
                if (temp['is_live'] == False and temp['is_cancelled'] == False):
                    b2eRecordBear[i][0] = 1
                    b2eRecordBear[i][6] = 1
                    b2eRecordBear[i][1] = 1
                    tt_bear = np.roll(tt_bear, -1)
                    tt_bear[-1] = time.time() - b2eRecordBear[i][7]
                    logger(fname, 'filled b2e bear trade has been confirmed')
                if (temp['is_live'] == True and temp['is_cancelled'] == False):
                    b2eRecordBear[i][0] = 1
                    logger(fname, 'b2e bear trade has been confirmed')
                if (int(b2eRecordBear[i][0]) == 1 and int(b2eRecordBear[i][6]) == 0):
                    rate = Algo.rate_linear_growth(fname, b2eRecordBear[i][1], time.time() - b2eRecordBear[i][7],
                                                   tt_bear_mean, tt_bear_mean + tt_bear_std, ema5_3m, ema10_3m, ethbtc)
                    if (rate != b2eRecordBear[i][4]):
                        if ((Gemcon.cancelOrder(b2eRecordBear[i][5]).status_code) == 200):
                            logger(fname, 'canceled b2e bear trade for new rate')
                            temp = Gemcon.orderStatus(b2eRecordBear[i][5])
                            if (temp.status_code == 200):
                                temp = temp.json()
                                logger(fname, temp)
                                if (float(temp['executed_amount']) == 0):
                                    order = Gemcon.newOrder(format(b2eRecordBear[i][3], '.6f'), format(rate, '.5f'),
                                                            'buy', None, 'ethbtc')
                                    logger(fname, order.json())
                                    b2eRecordBear[i][6] = 1
                                else:
                                    order = Gemcon.newOrder(temp['remaining_amount'], format(rate, '.5f'), 'buy', None,
                                                            'ethbtc')
                                    logger(fname, order.json())
                                    b2eRecordBear[i][6] = 1
                                if (order.status_code == 200):
                                    order = order.json()
                                    logger(fname, order)
                                    temp = np.array(
                                        [0, b2eRecordBear[i][1], b2eRecordBear[i][2], b2eRecordBear[i][3], rate,
                                         int(order['order_id']), 0, b2eRecordBear[i][7]])
                                    b2eRecordBear = np.vstack((b2eRecordBear, temp))
                                else:
                                    logger(fname, order.json())
                                    logger(fname, 'placing new rate order failed')
                                    temp = np.array(
                                        [0, b2eRecordBear[i][1], b2eRecordBear[i][2], b2eRecordBear[i][3], rate,
                                         int(temp['order_id']), 0, b2eRecordBear[i][7]])
                                    graveyardRecordBear = np.vstack((graveyardRecordBear, temp))
                            else:
                                b2eRecordBear[i][0] = 2
                                temp = np.array(b2eRecordBear[i])
                                graveyardRecordBear = np.vstack((graveyardRecordBear, temp))
                                b2eRecordBear[i][6] = 1
                                logger(fname, 'b2e bear order status failed after new rate')
                        else:
                            logger(fname, 'canceling b2e bear record for new rate failed')
            else:
                logger(fname, 'b2e record bear order status failed')


def geminiEthbtc(csvfile):
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(HEADER)
    print('Starting Gemini Bot')
    global active
    active = True
    ethOffset = float(linecache.getline('settings.cfg', 27).rstrip("\n\r"))
    btcOffset = float(linecache.getline('settings.cfg', 29).rstrip("\n\r"))
    ethReserved = 0
    btcReserved = 0
    recently = int(linecache.getline('settings.cfg', 25).rstrip("\n\r"))
    ethbtc = np.zeros(recently)
    vwap = np.zeros(recently)


    e2bRecordBear = pd.DataFrame(
        columns=['Confirmation', 'Reserved BTC', 'Time', 'Amount', 'Rate', 'ID', 'Delete'])
    b2eRecordBear = pd.DataFrame(
        columns=['Confirmation', 'Rate Needed', 'Time', 'Amount', 'Rate', 'ID', 'Delete', 'Pair Time'])

    graveyardRecordBear = e2bRecordBear


    b2eRecordBull = e2bRecordBear
    e2bRecordBull = e2bRecordBear
    graveyardRecordBull = e2bRecordBear


    mTimeEthbtc = 0
    hTimeEthbtc = 0


    timeLimit = 900
    global fee
    fee = 0.0025

    tt_bear_mean_init = float(linecache.getline('settings.cfg', 17).rstrip("\n\r"))
    tt_bear_std_init = float(linecache.getline('settings.cfg', 19).rstrip("\n\r"))
    tt_bear = np.asarray([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    tt_bull = tt_bear
    tt_bear[-1] = tt_bear_mean_init
    tt_bull_mean_init = float(linecache.getline('settings.cfg', 21).rstrip("\n\r"))
    tt_bull_std_init = float(linecache.getline('settings.cfg', 23).rstrip("\n\r"))
    tt_bull[-1] = tt_bull_mean_init

    global loggingEnable
    loggingEnable = str(linecache.getline('settings.cfg', 31)).rstrip("\n\r")
    fname = 'output_gemini_' + str(int(time.time())) + '.txt'
    if (loggingEnable == 'true'):
        f = open(fname, 'w')
        f.close()


    mTries = 10

    while (active):
        lockout = False
        tradeTimeArray = np.asarray([0,])
        timeNow = time.strftime('%H:%M:%S')
        timeNow2 = time.time()
        logger(fname, timeNow2)

        if (np.count_nonzero(tt_bear) == 0):
            tt_bear_mean, tt_bear_std = norm.fit(tt_bear)
        else:
            tt_bear_mean = tt_bear_mean_init
            tt_bear_std = tt_bear_std_init

        if (np.count_nonzero(tt_bull) == 0):
            tt_bull_mean, tt_bull_std = norm.fit(tt_bull)
        else:
            tt_bull_mean = tt_bull_mean_init
            tt_bull_std = tt_bull_std_init



        # get indicator data
        timeScale = 1  # in hours
        if (time.time() >= hTimeEthbtc + timeScale * 3600):
            try:
                hourResults = hourData
                logger(fname, 'new ethbtc hour data!')
            except Exception as e:
                logger(fname, 'ethbtc hour data try failed')
                logger(fname, e)
                lockout = True
        timeScale = 15  # in minutes

        if (time.time() >= mTimeEthbtc + timeScale * 60):
            try:
                minuteResults = minuteData('Gemini', 'ETH', str(timeScale), '500')
                logger(fname, 'new ethbtc minute data!')
            except Exception as e:
                lockout = True
                logger(fname, 'ethbtc minute data try failed')
                logger(fname, e)

        try:
            temp = geminiLastPrice()
            ethbtc = np.roll(ethbtc, -1)
            ethbtc[-1] = temp
        except Exception as e:
            logger(fname, e)
            logger(fname, 'gemini current price try failed')
            ethbtc = np.roll(ethbtc, -1)
            ethbtc[-1] = 0
            lockout = True

        ethReserved = e2bRecordBear["Reserved BTC"].sum()

        if (lockout == False):
            e2bSig = Algo.eth2btc_signal_15m_bear(fname, ethbtc, minuteResults, hourResults)
            logger(fname, 'bear e2bsig is ' + str(e2bSig))
            if (e2bSig):
                e2bBearTradeExecute(fname, btcOffset, ethOffset, ethReserved, ethbtc)

            e2bConfirmCancelOrders(fname, timeLimit)

            b2eBearTradeExecute(fname, ethbtc)

            b2eConfirmCancelUpdateOrders(fname)

            b2eRecordBear = b2eRecordBear[b2eRecordBear[:, 6] == 0]

            for i in range(0, len(graveyardRecordBear)):
                if (graveyardRecordBear[i][3] != 0 and graveyardRecordBear[i][0] == 0):
                    rate = Algo.rate_linear_growth(fname, graveyardRecordBear[i][1],
                                                   time.time() - graveyardRecordBear[i][7], tt_bear_mean,
                                                   tt_bear_mean + tt_bear_std, ema5_3m, ema10_3m, ethbtc)
                    order = Gemcon.newOrder(format(graveyardRecordBear[i][3], '.6f'), format(rate, '.5f'), 'buy', None,
                                            'ethbtc')
                    if (order.status_code == 200):
                        graveyardRecordBear[i][5] = int(order.json()['order_id'])
                        b2eRecordBear = np.vstack((b2eRecordBear, graveyardRecordBear[i]))
                        graveyardRecordBear[i][6] = 1
                        logger(fname, 'graveyard bear order resurrected')
                    else:
                        logger(fname, order.json())
                        logger(fname, 'graveyar bear order failed')
                if (graveyardRecordBear[i][3] != 0 and graveyardRecordBear[i][0] == 2):
                    rate = Algo.rate_linear_growth(fname, graveyardRecordBear[i][1],
                                                   time.time() - graveyardRecordBear[i][7], tt_bear_mean,
                                                   tt_bear_mean + tt_bear_std, ema5_3m, ema10_3m, ethbtc)
                    temp = Gemcon.orderStatus(graveyardRecordBear[i][5])
                    if (temp.status_code == 200):
                        temp = temp.json()
                        if (float(temp['executed_amount']) == 0):
                            order = Gemcon.newOrder(format(graveyardRecordBear[i][3]), format(rate, '.5f'), 'buy', None,
                                                    'ethbtc')
                            graveyardRecordBear[i][6] = 1
                            logger(fname, order.json())
                        else:
                            order = Gemcon.newOrder(temp['remaining_amount'], format(rate, '.5f'), 'buy', None,
                                                    'ethbtc')
                            graveyardRecordBear[i][6] = 1
                            logger(fname, order.json())
                        if (order.status_code == 200):
                            order = order.json()
                            temp = np.array(
                                [0, graveyardRecordBear[i][1], graveyardRecordBear[i][2], graveyardRecordBear[i][3],
                                 rate, int(order['order_id']), 0, graveyardRecordBear[i][7]])
                            b2eRecordBear = np.vstack(b2eRecordBear, temp)
                            logger(fname, 'graveyard bear [i][0] order resurrected')

                        else:
                            logger(fname, order.json())
                            logger(fname, 'graveyard bear [i][0] = 2 order failed')
                    else:
                        logger(fname, 'graveyard bear order status failed')

            graveyardRecordBear = graveyardRecordBear[graveyardRecordBear[:, 6] == 0]

            logger(fname, 'e2b record bear:')
            logger(fname, e2bRecordBear)
            logger(fname, 'b2e record bear:')
            logger(fname, b2eRecordBear)
            logger(fname, 'graveyardRecordBear:')
            logger(fname, graveyardRecordBear)
            logger(fname, 'TICK')

            csv_writer.writerow(
                [timeNow, timeNow2, ethbtc[-1], mOpen, mClose, mHigh, mLow, mvolfrom, mVolTo, vwap[-1],
                 williams14h, williams14m, ema5m, ema10m,
                 dema5, dema10, macd, macdsignal, sls, rsi, uosc,
                 adx, stocfastk, stocfastd, aroon, di_plus, di_minus, eth, btc, e2bsig, vwapCross, ema5_3m, ema10_3m])
            csvfile.flush()
        else:
            logger(fname, 'lockout bear is active')

        btcReserved = np.sum(e2bRecordBear, 0)[1]

        if (lockout == False):
            b2esig = Algo.btc2eth_signal_15m_bull(fname, ethbtc, minuteResults, hourResults)
            # print 'vwapCross is', vwapCross
            logger(fname, 'bull b2esig is ' + str(b2esig))
            if (np.count_nonzero(ethbtc) == recently and np.count_nonzero(vwap) == recently):
                logger(fname, 'window ready!')
                if (b2esig == True):
                    balance = Gemcon.balances()
                    if (balance.status_code == 200):
                        balance = balance.json()
                        for i in range(0, len(balance)):
                            if (balance[i]['currency'] == 'BTC'):
                                btc = float(
                                    balance[i]['available']) + btcOffset
                            if (balance[i]['currency'] == 'USD'):
                                usd = float(balance[i]['available'])
                            if (balance[i]['currency'] == 'ETH'):
                                eth = float(balance[i]['available']) + ethOffset
                                eth = roundDown(eth, 6)
                                logger(fname, eth)
                    else:
                        eth = 0
                        btc = 0
                        usd = 0

                    if (btc - btcReserved > 0.00001):  # if we have enough btc
                        market = json.loads(Gemcon.book('ethbtc'))
                        for i in range(0, len(market['asks'])):
                            ask_rate = np.round(float(market['asks'][i]['price']), 5)
                            ask_amount = roundDown(float(market['asks'][i]['amount']), 6)
                            if (ask_rate / ethbtc[
                                -1] <= 1.01 and btc - btcReserved > 0.00001):  # if order book is close to our target
                                total = np.round((ask_rate * ask_amount * (1 + fee)), 5)
                                if ((btc - btcReserved) < total):
                                    # execute trade
                                    amount = ((btc - btcReserved) * (1 - fee)) / (ask_rate)
                                    amount = roundDown(amount, 6)
                                    logger(fname, 'selling all btc for eth')
                                    try:
                                        order = Gemcon.newOrder(format(amount, '.6f'), format(ask_rate, '.5f'), 'buy',
                                                                None, 'ethbtc')
                                        if (order.status_code == 200):
                                            order = order.json()
                                            temp = np.array(
                                                [0, 0, time.time(), amount, ask_rate, int(order['order_id']), 0, 0])
                                            # [confirmation 0, reserved 1, time 2, amount 3, rate 4, id 5, delete 6, spare 7]
                                            total = amount * ask_rate * (1 + fee)
                                            b2eRecordBull = np.vstack((b2eRecordBull, temp))
                                        else:
                                            logger(fname, 'b2e order failed')
                                            logger(fname, order)
                                    except Exception as e:
                                        logger(fname, e)
                                        logger(fname, 'b2e order try failed')
                                else:
                                    logger(fname, 'selling some btc for eth')
                                    try:
                                        order = Gemcon.newOrder(format(total, '.6f'), format(ask_rate, '.5f'), 'buy',
                                                                None, 'ethbtc')
                                        if (order.status_code == 200):
                                            order = order.json()
                                            temp = np.array(
                                                [0, 0, time.time(), total, ask_rate, int(order['order_id']), 0, 0])
                                            # [confirmation 0, reserved eth 1, time 2, amount 3, rate 4, id 5, delete 6, spare 7]
                                            b2eRecordBull = np.vstack((b2eRecordBull, temp))
                                            total = ask_rate * ask_amount * (1 + fee)
                                        else:
                                            logger(fname, 'b2e order failed')
                                            logger(fname, order)
                                    except Exception as e:
                                        logger(fname, e)
                                        logger(fname, 'b2e order try failed')

                                balance = Gemcon.balances()
                                if (balance.status_code == 200):
                                    balance = balance.json()
                                    for i in range(0, len(balance)):
                                        if (balance[i]['currency'] == 'BTC'):
                                            btc = float(
                                                balance[i]['available']) + btcOffset
                                        if (balance[i]['currency'] == 'USD'):
                                            usd = float(balance[i]['available'])
                                        if (balance[i]['currency'] == 'ETH'):
                                            eth = float(balance[i]['available']) + ethOffset
                                            eth = roundDown(eth, 6)
                                            logger(fname, eth)
                                else:
                                    eth = 0
                                    btc = 0
                                    usd = 0
                            else:
                                logger(fname, 'order book not close enough')
                                break
                    else:
                        logger(fname, 'not enough btc')

            for i in range(0, len(b2eRecordBull)):
                if (int(b2eRecordBull[i][5]) != 0):
                    if (int(b2eRecordBull[i][0]) == 0):
                        temp = Gemcon.orderStatus(b2eRecordBull[i][5])
                        if (temp.status_code == 200):
                            temp = temp.json()
                            logger(fname, temp)
                            logger(fname, time.time() - int(temp['timestamp']))
                            if (temp['is_live'] == False and temp['is_cancelled'] == True and temp[
                                'executed_amount'] == '0'):
                                b2eRecordBull[i][6] = 1
                            if (temp['is_live'] == False and temp['is_cancelled'] == False):
                                b2eRecordBull[i][0] = 1
                                b2eRecordBull[i][1] = b2eRecordBull[i][3]
                                b2eRecordBull[i][4] = float(temp['avg_execution_price'])
                                logger(fname, 'filled b2e bull trade has been confirmed')
                            if (temp['is_live'] == True and temp['is_cancelled'] == False):
                                if (time.time() - int(temp['timestamp']) > timeLimit):
                                    if (Gemcon.cancelOrder(b2eRecordBull[i][5]).status_code == 200):
                                        temp = Gemcon.orderStatus(b2eRecordBull[i][5])
                                        if (temp.status_code == 200):
                                            temp = temp.json()
                                            if (float(temp['executed_amount']) == 0):
                                                b2eRecordBull[i][6] = 1
                                                logger(fname, 'canceled bammer order')
                                            else:
                                                b2eRecordBull[i][3] = float(temp['executed_amount'])
                                                b2eRecordBull[i][0] = 1
                                                b2eRecordBull[i][1] = b2eRecordBull[i][3]
                                                logger(fname, 'canceled partially filled b2e bull order')
                                        else:
                                            b2eRecordBull[i][0] = 2
                                            logger(fname, 'b2e bull order status (2nd round) failed')
                                    else:
                                        logger(fname, 'canceling partial b2e bull order failed')
                                        b2eRecordBull[i][1] = b2eRecordBull[i][3]
                        else:
                            logger(fname, 'b2e bull order status failed')
                    if (b2eRecordBull[i][0] == 2):
                        temp = Gemcon.orderStatus(b2eRecordBull[i][5])
                        if (temp.status_code == 200):
                            temp = temp.json()
                            if (float(temp['executed_amount']) == 0):
                                b2eRecordBull[i][6] = 1
                                logger(fname, 'canceled bammer order')
                            else:
                                b2eRecordBull[i][3] = float(temp['executed_amount'])
                                b2eRecordBull[i][0] = 1
                                b2eRecordBull[i][1] = b2eRecordBull[i][3]
                                logger(fname, 'canceled partially filled b2e bull order')

            b2eRecordBull = b2eRecordBull[b2eRecordBull[:, 6] == 0]

            for i in range(0, len(b2eRecordBull)):
                e2b = Algo.eth2btc_signal_with_decay_bull(fname, ethbtc, ema5m, ema10m, b2eRecordBull[i][3],
                                                          b2eRecordBull[i][4], time.time() - b2eRecordBull[i][2],
                                                          ema5_3m, ema10_3m, tt_bull_mean, tt_bull_mean + tt_bull_std)
                if (b2eRecordBull[i][0] == 1 and e2b[0] == 1):
                    logger(fname, 'eth 2 btc bull signal')
                    order = Gemcon.newOrder(format(b2eRecordBull[i][3], '.6f'), format(ethbtc[-1], '.5f'), 'sell', None,
                                            'ethbtc')
                    if (order.status_code == 200):
                        order = order.json()
                        temp = np.array(
                            [0, e2b[1], time.time(), b2eRecordBull[i][3], ethbtc[-1], int(order['order_id']), 0,
                             b2eRecordBull[i][2]])
                        # [confirmation 0,  rn 1, time 2, amount 3, rate 4, id 5, delete 6, pair time 7]
                        e2bRecordBull = np.vstack((e2bRecordBull, temp))
                        b2eRecordBull[i][6] = 1
                    else:
                        logger(fname, 'e2b bull trade failed')

            b2eRecordBull = b2eRecordBull[b2eRecordBull[:, 6] == 0]

            for i in range(0, len(e2bRecordBull)):
                if (int(e2bRecordBull[i][5]) != 0 and int(e2bRecordBull[i][5]) != 2):
                    temp = Gemcon.orderStatus(e2bRecordBull[i][5])
                    if (temp.status_code == 200):
                        temp = temp.json()
                        if (temp['is_live'] == False and temp['is_cancelled'] == False):
                            e2bRecordBull[i][0] = 1
                            e2bRecordBull[i][6] = 1
                            e2bRecordBull[i][1] = 1
                            tt_bull = np.roll(tt_bull, -1)
                            tt_bull[-1] = time.time() - e2bRecordBull[i][7]
                            logger(fname, 'filled e2b bull trade has been confirmed')
                        if (temp['is_live'] == True and temp['is_cancelled'] == False):
                            e2bRecordBull[i][0] = 1
                            logger(fname, 'e2b bull trade has been confirmed')
                        if (int(e2bRecordBull[i][0]) == 1 and int(e2bRecordBull[i][6]) == 0):
                            rate = Algo.rate_linear_decay(fname, e2bRecordBull[i][1], time.time() - e2bRecordBull[i][7],
                                                          tt_bull_mean, tt_bull_mean + tt_bull_std, ema5_3m, ema10_3m,
                                                          ethbtc)
                            if (rate != e2bRecordBull[i][4]):
                                if ((Gemcon.cancelOrder(e2bRecordBull[i][5]).status_code) == 200):
                                    temp = Gemcon.orderStatus(e2bRecordBull[i][5])
                                    if (temp.status_code == 200):
                                        temp = temp.json()
                                        if (float(temp['executed_amount']) == 0):
                                            order = Gemcon.newOrder(format(e2bRecordBull[i][3], '.6f'),
                                                                    format(rate, '.5f'), 'sell', None, 'ethbtc')
                                            e2bRecordBull[i][6] = 1
                                        else:
                                            order = Gemcon.newOrder(temp['remaining_amount'], format(rate, '.5f'),
                                                                    'sell', None, 'ethbtc')
                                            e2bRecordBull[i][6] = 1
                                        if (order.status_code == 200):
                                            order = order.json()
                                            temp = np.array(
                                                [0, e2bRecordBull[i][1], e2bRecordBull[i][2], e2bRecordBull[i][3], rate,
                                                 int(order['order_id']), 0, e2bRecordBull[i][7]])
                                            e2bRecordBull = np.vstack((e2bRecordBull, temp))
                                        else:
                                            logger(fname, 'placing new rate bull order failed')
                                            temp = np.array(
                                                [0, e2bRecordBull[i][1], e2bRecordBull[i][2], e2bRecordBull[i][3], rate,
                                                 int(temp['order_id']), 0, e2bRecordBull[i][7]])
                                            graveyardRecordBull = np.vstack((graveyardRecordBull, temp))
                                    else:
                                        e2bRecordBull[i][0] = 2
                                        temp = np.array(e2bRecordBull[i])
                                        graveyardRecordBull = np.vstack((graveyardRecordBull, temp))
                                        e2bRecordBull[i][6] = 1
                                        logger(fname, 'e2b bull order status failed after new rate')
                                else:
                                    logger(fname, 'canceling e2b bull record for new rate failed')
                    else:
                        logger(fname, 'e2b record bull order status failed')

            e2bRecordBull = e2bRecordBull[e2bRecordBull[:, 6] == 0]

            for i in range(0, len(graveyardRecordBull)):
                if (graveyardRecordBull[i][3] != 0 and graveyardRecordBull[i][0] == 0):
                    rate = Algo.rate_linear_decay(fname, graveyardRecordBull[i][1],
                                                  time.time() - graveyardRecordBull[i][7], tt_bull_mean,
                                                  tt_bull_mean + tt_bull_std, ema5_3m, ema10_3m, ethbtc)
                    order = Gemcon.newOrder(format(graveyardRecordBull[i][3], '.6f'), format(rate, '.5f'), 'sell', None,
                                            'ethbtc')
                    if (order.status_code == 200):
                        graveyardRecordBull[i][5] = int(order.json()['order_id'])
                        e2bRecordBull = np.vstack((e2bRecordBull, graveyardRecordBull[i]))
                        graveyardRecordBull[i][6] = 1
                        logger(fname, 'graveyard record bull order resurrected')
                    else:
                        logger(fname, order.json())
                        logger(fname, 'graveyard record bull order failed')
                if (graveyardRecordBull[i][3] != 0 and graveyardRecordBull[i][0] == 2):
                    rate = Algo.rate_linear_decay(fname, graveyardRecordBull[i][1],
                                                  time.time() - graveyardRecordBull[i][2], tt_bull_mean,
                                                  tt_bull_mean + tt_bull_std, ema5_3m, ema10_3m, ethbtc)
                    temp = Gemcon.orderStatus(graveyardRecordBull[i][5])
                    if (temp.status_code == 200):
                        temp = temp.json()
                        if (float(temp['executed_amount']) == 0):
                            order = Gemcon.newOrder(format(graveyardRecordBull[i][3], '.6f'), format(rate, '.5f'),
                                                    'sell', None, 'ethbtc')
                            graveyardRecordBull[i][6] = 1
                        else:
                            order = Gemcon.newOrder(temp['remaining_amount'], format(rate, '.5f'), 'sell', None,
                                                    'ethbtc')
                            graveyardRecordBull[i][6] = 1
                        if (order.status_code == 200):
                            order = order.json()
                            temp = np.array(
                                [0, graveyardRecordBull[i][1], graveyardRecordBull[i][2], graveyardRecordBull[i][3],
                                 rate, int(order['order_id']), 0, graveyardRecordBull[i][7]])
                            e2bRecordBull = np.vstack((graveyardRecordBull[i], temp))
                        else:
                            logger(fname, order.json())
                            logger(fname, 'graveyard [i][1] = 2 order failed')
                    else:
                        logger(fname, 'graveyard bull order status failed')

            graveyardRecordBull = graveyardRecordBull[graveyardRecordBull[:, 6] == 0]

            logger(fname, 'e2b record bull:')
            logger(fname, e2bRecordBull)
            logger(fname, 'b2e record bull:')
            logger(fname, b2eRecordBull)
            logger(fname, 'graveyard bull:')
            logger(fname, graveyardRecordBull)
            logger(fname, 'TOCK')
            logger(fname, '\n')

            time.sleep(3)

        else:
            logger(fname, 'lockout bull is active')


with open('results_' + str(time.time()) + '.csv', 'wb') as csvfile:
    gemini_Ethbtc(csvfile)
