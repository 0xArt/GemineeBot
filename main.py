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
    d = tickers.json()
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


def checkIfCsvEmpty(csvfile):
    reader_file = csv.reader(csvfile)
    value = len(list(reader_file))
    if(value  == 0):
        return True
    return False


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
    df.loc[len(df)] = data
    return df

def deleteRows(df):
    return df[df.Delete != 'True'].reset_index(drop=True)


def e2bBearTradeExecute(fname, btcOffset, ethOffset, ethReserved, ethbtc):
    global fee
    global e2bRecordBear
    eth = 0

    # check and update balance

    balance = Gemcon.balances()
    print("BALANCE")
    print(balance)
    if (balance.status_code == 200):
        balance = balance.json()

        logger(fname,balance)
        for i in range(0, len(balance)):
            if (balance[i]['currency'] == 'ETH'):
                eth = float(balance[i]['available']) + ethOffset
                eth = roundDown(eth, 6)
    else:
        eth = 0


    # if we have enough eth
    if (eth - ethReserved > 0.001):
        market = Gemcon.book('ethbtc').json()
        # iteratively scan order book
        for i in range(0, len(market['bids'])):
            bid_rate = np.round(float(market['bids'][i]['price']), 5)
            bid_amount = roundDown(float(market['bids'][i]['amount']), 6)
            # if order book is close to our target
            if (bid_rate / ethbtc > 0.99):
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
                                         int(order['order_id']), 'False']
                        # [confirmation 0, reserved btc 1, time 2, amount 3, rate 4, id 5, delete 6]
                        print(e2bRecordBear)
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
                    e2bRecordBear["Reserved"][i] = e2bRecordBear['Amount'][i] * e2bRecordBear['Rate'][i] * (1 - fee)
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
                                    e2bRecordBear['Delete'][i] = 'True'
                                    e2bRecordBear["Reserved"][i] = e2bRecordBear['Amount'][i] * e2bRecordBear['Rate'][i] * (1 - fee)
                                    logger(fname, 'canceled partially filled e2b bear order')
                            else:
                                e2bRecordBear["Confirmation"][i] = "Fail"
                                logger(fname, 'e2b bear order status (2nd round) failed')
                        # if canceling timed out order fails, reserve full amount of btc just in case
                        else:
                            logger(fname, 'canceling timed out b2e bear order failed')
                            e2bRecordBear["Reserved"][i] = e2bRecordBear['Amount'][i] * e2bRecordBear['Rate'][i] * (1 - fee)
            else:
                # if checking order status fails do nothing
                logger(fname, 'e2b bear order status failed')


            if (e2bRecordBear["Confirmation"][i] == "Fail"):
                temp = Gemcon.orderStatus(e2bRecordBear["ID"][i])
                if (temp.status_code == 200):
                    temp = temp.json()
                    if (float(temp['executed_amount']) == 0):
                        e2bRecordBear["Delete"][i] = "True"
                        logger(fname, 'canceled bammer order')
                    else:
                        e2bRecordBear["Amount"][i] = float(temp['executed_amount'])
                        e2bRecordBear["Confirmation"][i] = "True"
                        e2bRecordBear["Reserved"][i] = e2bRecordBear["Amount"][i] * e2bRecordBear["Rate"][i] * (1 - fee)
                        logger(fname, 'canceled partially filled e2b bear order')
                else:
                    logger(fname, 'e2b bear order status failed')


    e2bRecordBear = deleteRows(e2bRecordBear)


def b2eBearTradeExecute(fname, ethbtc, fastMinuteResults):
    global e2bRecordBear
    global b2eRecordBear
    global averageBearTradeTime
    global averageBearTradeTimeStd

    for i in range(0, len(e2bRecordBear)):
        # if e2b order is confirmed, check for trade back signal
        if(e2bRecordBear['Confirmation'][i] == 'True'):
            # check for signal
            b2e = Algo.btc2ethSignalWithGrowthBear(fname, ethbtc, e2bRecordBear['Amount'][i],
                                                       e2bRecordBear['Rate'][i], time.time() - e2bRecordBear['Time'][i],
                                                   fastMinuteResults, averageBearTradeTime, averageBearTradeTime + averageBearTradeTimeStd )
            if (b2e):
                logger(fname, 'btc 2 eth bear signal')
                # swap back to eth
                order = Gemcon.newOrder(format(e2bRecordBear['Amount'][i], '.6f'), format(ethbtc[-1], '.5f'), 'buy', None, 'ethbtc')
                if (order.status_code == 200):
                    order = order.json()
                    logger(fname, order)
                    temp = ["False", Algo.rateNeededBear(float(order['original_amount']), float(order['price'])), time.time(), float(order['original_amount']), float(order['price']), int(order['order_id']), 0, e2bRecordBear['Time'][i]]
                    # [confirmation 0,  rn 1, time 2, amount 3, rate 4, id 5, delete 6,  pair time 7]
                    b2eRecordBear = addRow((b2eRecordBear, temp))
                    e2bRecordBear['Delete'][i] = 'True'
                else:
                    logger(fname, 'b2e bear trade failed')

    e2bRecordBear = deleteRows(e2bRecordBear)



def b2eConfirmCancelUpdateOrders(fname, ethbtc, fastMinuteResults):
    global b2eRecordBear
    global graveyardRecordBear
    global tradeTimeBear
    global averageBearTradeTime
    global stdBearTradeTime

    for i in range(0, len(b2eRecordBear)):
        temp = Gemcon.orderStatus(b2eRecordBear["ID"][i])
        if (temp.status_code == 200):
            temp = temp.json()
            if (temp['is_live'] == False and temp['is_cancelled'] == False):
                b2eRecordBear['Confirmation'][i] = 'True'
                b2eRecordBear['Delete'][i] = 'True'
                tradeTimeBear = np.roll(tradeTimeBear, -1)
                tradeTimeBear[-1] = time.time() - b2eRecordBear['Pair Time'][i]
                logger(fname, 'filled b2e bear trade has been confirmed')
            if (temp['is_live'] == True and temp['is_cancelled'] == False):
                b2eRecordBear['Confirmation'][i] = 'True'
                logger(fname, 'b2e bear trade has been confirmed')
            if (b2eRecordBear['Confirmation'][i] == 'True' and b2eRecordBear['Delete'][i] == 'False'):
                '''
                rate = Algo.rate_linear_growth(fname, b2eRecordBear[i][1], time.time() - b2eRecordBear[i][7],
                                               tt_bear_mean, tt_bear_mean + tt_bear_std, ema5_3m, ema10_3m, ethbtc)
               '''
                rate = Algo.rateLinearGrowth(fname, b2eRecordBear['Rate'][i], time.time() - b2eRecordBear['Pair Time'][i], averageBearTradeTime, averageBearTradeTime + stdBearTradeTime, fastMinuteResults, ethbtc)
                if (rate != float(b2eRecordBear['Rate'][i])):
                    if ((Gemcon.cancelOrder(b2eRecordBear['ID'][i])).status_code == 200):
                        logger(fname, 'canceled b2e bear trade for new rate')
                        temp = Gemcon.orderStatus(b2eRecordBear['ID'][i])
                        if (temp.status_code == 200):
                            temp = temp.json()
                            logger(fname, temp)
                            if (float(temp['executed_amount']) == 0):
                                order = Gemcon.newOrder(format(b2eRecordBear['Amount'][i], '.6f'), format(rate, '.5f'),
                                                        'buy', None, 'ethbtc')
                                logger(fname, order.json())
                                b2eRecordBear['Delete'][i] = 'True'
                            else:
                                order = Gemcon.newOrder(temp['remaining_amount'], format(rate, '.5f'), 'buy', None,
                                                        'ethbtc')
                                logger(fname, order.json())
                                b2eRecordBear['Delete'][i] = 'True'
                            if (order.status_code == 200):
                                order = order.json()
                                logger(fname, order)
                                temp = ["False", b2eRecordBear['Rate Needed'][i], b2eRecordBear['Time'][i], b2eRecordBear['Amount'][i], rate,
                                     int(order['order_id']), 'False', b2eRecordBear['Pair Time'][i]]
                                b2eRecordBear = addRow(b2eRecordBear, temp)
                            else:
                                logger(fname, order.json())
                                logger(fname, 'placing new rate order failed')
                                temp = ["False", b2eRecordBear['Rate Needed'][i], b2eRecordBear['Time'][i], b2eRecordBear['Amount'][i], rate,
                                     int(order['order_id']), 'False', b2eRecordBear['Pair Time'][i]]
                                graveyardRecordBear = addRow(graveyardRecordBear, temp)
                        else:
                            b2eRecordBear["Confirmation"][i] = "Fail"
                            temp = b2eRecordBear.iloc[i, :]
                            graveyardRecordBear = addRow(graveyardRecordBear, temp)
                            b2eRecordBear["Delete"][i] = "True"
                            logger(fname, 'b2e bear order status failed after new rate')
                    else:
                        logger(fname, 'canceling b2e bear record for new rate failed')
        else:
            logger(fname, 'b2e record bear order status failed')

    b2eRecordBear = deleteRows(b2eRecordBear)


def sweepBearGraveyard(fname, ethbtc, fastMinuteResults):
    global graveyardRecordBear
    global b2eRecordBear
    global averageBearTradeTime
    global stdBearTradeTime


    for i in range(0, len(graveyardRecordBear)):
        if (graveyardRecordBear['Confirmation'][i] == 'False'):
            rate = Algo.rateLinearGrowth(fname, graveyardRecordBear['Rate Needed'][i],
                                         time.time() - graveyardRecordBear['Pair Time'][i], averageBearTradeTime,
                                         averageBearTradeTime + stdBearTradeTime, fastMinuteResults, ethbtc)
            order = Gemcon.newOrder(format(graveyardRecordBear["Amount"][i], '.6f'), format(rate, '.5f'), 'buy', None,
                                    'ethbtc')
            if (order.status_code == 200):
                graveyardRecordBear['ID'][i] = int(order.json()['order_id'])
                b2eRecordBear = addRow(b2eRecordBear, graveyardRecordBear.iloc[[i]])
                graveyardRecordBear['Delete'][i] = "True"
                logger(fname, 'graveyard bear order resurrected')
            else:
                logger(fname, order.json())
                logger(fname, 'graveyard bear order failed')
        if (graveyardRecordBear['Confirmation'][i] == 'Fail'):
            rate = Algo.rateLinearGrowth(fname, graveyardRecordBear['Rate Needed'][i],
                                         time.time() - graveyardRecordBear['Pair Time'][i], averageBearTradeTime,
                                         averageBearTradeTime + stdBearTradeTime, fastMinuteResults, ethbtc)
            temp = Gemcon.orderStatus(graveyardRecordBear["ID"][i])
            if (temp.status_code == 200):
                temp = temp.json()
                amount = temp['remaining_amount']
                if (float(temp['executed_amount']) == 0):
                    order = Gemcon.newOrder(amount, format(rate, ".5f"), "buy", None, "ethbtc")
                    graveyardRecordBear["Delete"][i] = "True"
                    logger(fname, order.json())
                else:
                    order = Gemcon.newOrder(amount, format(rate, '.5f'), 'buy', None,
                                            'ethbtc')
                    logger(fname, order.json())
                if (order.status_code == 200):
                    graveyardRecordBear["Delete"][i] = "True"
                    order = order.json()
                    temp = ["False", graveyardRecordBear['Rate Needed'][i], graveyardRecordBear['Time'][i], amount, rate, int(order['order_id']), 'False', graveyardRecordBear['Pair Time'][i]]
                    b2eRecordBear = addRow(b2eRecordBear, temp)
                    logger(fname, 'graveyard bear [i][0] order resurrected')

                else:
                    logger(fname, order.json())
                    logger(fname, 'graveyard bear [i][0] = 2 order failed')
            else:
                logger(fname, 'graveyard bear order status failed')

    graveyardRecordBear = deleteRows(graveyardRecordBear)

def b2eBullTradeExecute(fname, btcOffset, ethOffset, btcReserved, ethbtc):
    global b2eRecordBull

    try:
        balance = Gemcon.balances()
        if (balance.status_code == 200):
            balance = balance.json()
            for i in range(0, len(balance)):
                if (balance[i]['currency'] == 'BTC'):
                    btc = float(
                        balance[i]['available']) + btcOffset
                    logger(fname, btc)
        else:
            btc = 0
    except Exception as e:
        logger(fname, 'balance request try fail')
        logger(fname, e)
        btc = 0

    #if we have enought btc
    if (btc - btcReserved > 0.00001):  # if we have enough btc
        market = json.loads(Gemcon.book('ethbtc'))
        # iteratively scan order book
        for i in range(0, len(market['asks'])):
            ask_rate = np.round(float(market['asks'][i]['price']), 5)
            ask_amount = roundDown(float(market['asks'][i]['amount']), 6)
            # if order book is close to our target
            if (ask_rate / ethbtc[-1] <= 1.01 and btc - btcReserved > 0.00001):
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
                            temp = ['False', 0, time.time(), float(order['original_amount']), float(order['price']),
                                    int(order['order_id']), 'False']
                            # [confirmation 0, reserved 1, time 2, amount 3, rate 4, id 5, delete 6, spare 7]
                            b2eRecordBull = addRow(b2eRecordBull, temp)
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
                            temp = ['False', 0, time.time(), float(order['original_amount']), float(order['price']),
                                    int(order['order_id']), 'False']
                            # [confirmation 0, reserved 1, time 2, amount 3, rate 4, id 5, delete 6, spare 7]
                            b2eRecordBull = addRow(b2eRecordBull, temp)
                        else:
                            logger(fname, 'b2e order failed')
                            logger(fname, order)
                    except Exception as e:
                        logger(fname, e)
                        logger(fname, 'b2e order try failed')
            else:
                logger(fname, 'order book not close enough')
                break
    else:
        logger(fname, 'not enough btc')

def b2eConfirmCancelOrders(fname, timeLimit):
    global b2eRecordBull
    global fee

    for i in range(0, len(b2eRecordBull)):
        if (b2eRecordBull["Confirmation"][i] == 'False'):
            temp = Gemcon.orderStatus(b2eRecordBull['ID'][i])
            if (temp.status_code == 200):
                temp = temp.json()
                logger(fname, temp)
                logger(fname, time.time() - int(temp['timestamp']))
                if (temp['is_live'] == False and temp['is_cancelled'] == True and temp[
                    'executed_amount'] == '0'):
                    b2eRecordBull['Delete'][i] = 'True'
                if (temp['is_live'] == False and temp['is_cancelled'] == False):
                    b2eRecordBull["Confirmation"][i] = 'True'
                    b2eRecordBull["Reserved"][i] = b2eRecordBull["Amount"][i]
                    b2eRecordBull["Rate"][i] = float(temp['avg_execution_price'])
                    logger(fname, 'filled b2e bull trade has been confirmed')
                if (temp['is_live'] == True and temp['is_cancelled'] == False):
                    if (time.time() - int(temp['timestamp']) > timeLimit):
                        if (Gemcon.cancelOrder(b2eRecordBull["ID"][i]).status_code == 200):
                            temp = Gemcon.orderStatus(b2eRecordBull["ID"][i])
                            if (temp.status_code == 200):
                                temp = temp.json()
                                if (float(temp['executed_amount']) == 0):
                                    b2eRecordBull["Delete"][i] = "True"
                                    logger(fname, 'canceled bammer order')
                                else:
                                    b2eRecordBull["Amount"][i] = float(temp['executed_amount'])
                                    b2eRecordBull["Delete"][i] = "True"
                                    b2eRecordBull["Reserved"][i] = b2eRecordBull["Amount"][i]
                                    logger(fname, 'canceled partially filled b2e bull order')
                            else:
                                b2eRecordBull["Confirmation"][i] = "Fail"
                                logger(fname, 'b2e bull order status (2nd round) failed')
                        else:
                            logger(fname, 'canceling partial b2e bull order failed')
                            b2eRecordBull["Reserved"][i] = b2eRecordBull["Amount"][i]
            else:
                logger(fname, 'b2e bull order status failed')

        if (b2eRecordBull["Confirmation"][i] == 'Fail'):
            temp = Gemcon.orderStatus(b2eRecordBull["ID"][i])
            if (temp.status_code == 200):
                temp = temp.json()
                if (float(temp['executed_amount']) == 0):
                    b2eRecordBull["Delete"][i] = "True"
                    logger(fname, 'canceled bammer order')
                else:
                    b2eRecordBull["Amount"][i] = float(temp['executed_amount'])
                    b2eRecordBull["Confirmation"][i] = "True"
                    b2eRecordBull["Reserved"][i] = b2eRecordBull["Amount"][i]
                    logger(fname, 'canceled partially filled b2e bull order')

    b2eRecordBull = deleteRows(b2eRecordBull)

def e2bBullTradeExecute(fname, ethbtc, fastMinuteResults):
    global b2eRecordBull
    global e2bRecordBull
    global averageBullTradeTime
    global stdBullTradeTime

    for i in range(0, len(b2eRecordBull)):
        e2b = Algo.eth2btcSignalWithDecayBull(fname, ethbtc, b2eRecordBull['Amount'][i],
                                     b2eRecordBull['Rate'][i], time.time() - b2eRecordBull['Time'][i],
                                                   fastMinuteResults, averageBullTradeTime, averageBullTradeTime + stdBullTradeTime )
        if (e2b):
            logger(fname, 'eth 2 btc bull signal')
            order = Gemcon.newOrder(format(b2eRecordBull[i][3], '.6f'), format(ethbtc[-1], '.5f'), 'sell', None,
                                    'ethbtc')
            if (order.status_code == 200):
                order = order.json()
                temp = ["False", Algo.rateNeededBull(b2eRecordBull['Amount'][i], b2eRecordBull['Rate'][i]), time.time(), float(order['original_amount']), float(order['price']),
                        int(order['order_id']), "False", b2eRecordBull['Time'][i]]
                # [confirmation 0,  rn 1, time 2, amount 3, rate 4, id 5, delete 6, pair time 7]
                e2bRecordBull = addRow(e2bRecordBull, temp)
                b2eRecordBull["Delete"][i] = "True"
            else:
                logger(fname, 'e2b bull trade failed')

    b2eRecordBull = deleteRows(b2eRecordBull)

def e2bConfirmCancelUpdateOrders(fname, ethbtc, fastMinuteResults):
    global e2bRecordBull
    global graveyardRecordBull
    global tradeTimeBull
    global averageBullTradeTime
    global stdBullTradeTime

    for i in range(0, len(e2bRecordBull)):
        temp = Gemcon.orderStatus(e2bRecordBull["ID"][i])
        if (temp.status_code == 200):
            temp = temp.json()
            if (temp['is_live'] == False and temp['is_cancelled'] == False):
                e2bRecordBull['Confirmation'][i] = 'True'
                e2bRecordBull['Delete'][i] = 'True'
                #e2bRecordBull[i][1] = 1 IDK WHAT THIS IS FOR
                tradeTimeBull = np.roll(tradeTimeBull, -1)
                tradeTimeBull[-1] = time.time() - e2bRecordBull['Pair Time'][i]
                logger(fname, 'filled e2b bull trade has been confirmed')
            if (temp['is_live'] == True and temp['is_cancelled'] == False):
                e2bRecordBull['Confirmation'][i] = 'True'
                logger(fname, 'e2b bull trade has been confirmed')
            if (e2bRecordBull['Confirmation'][i] == 'True' and e2bRecordBull['Delete'][i] == 'False'):
                rate = Algo.rateLinearDecay(fname, e2bRecordBull['Rate Needed'][i], time.time() - e2bRecordBull['Pair Time'][i], averageBullTradeTime, averageBullTradeTime + stdBullTradeTime, fastMinuteResults, ethbtc)
                if (rate != float(e2bRecordBull['Rate'][i])):
                    if ((Gemcon.cancelOrder(e2bRecordBull['ID'][i])).status_code == 200):
                        temp = Gemcon.orderStatus(e2bRecordBull['ID'][i])
                        if (temp.status_code == 200):
                            temp = temp.json()
                            if (float(temp['executed_amount']) == 0):
                                order = Gemcon.newOrder(format(e2bRecordBull["Amount"][i], '.6f'),
                                                        format(rate, '.5f'), 'sell', None, 'ethbtc')
                                e2bRecordBull["Confirmation"][i] = "True"
                            else:
                                order = Gemcon.newOrder(temp['remaining_amount'], format(rate, '.5f'),
                                                        'sell', None, 'ethbtc')
                                e2bRecordBull["Delete"][i] = "True"
                            if (order.status_code == 200):
                                order = order.json()
                                temp = ["False", e2bRecordBull['Rate Needed'][i], e2bRecordBull['Time'][i], e2bRecordBull['Amount'][i], rate,
                                     int(order['order_id']), 'False', e2bRecordBull['Pair Time'][i]]
                                e2bRecordBull = addRow(e2bRecordBull, temp)
                            else:
                                logger(fname, 'placing new rate bull order failed')
                                temp = ["False", e2bRecordBull['Rate Needed'][i], e2bRecordBull['Time'][i], e2bRecordBull['Amount'][i], rate,
                                     int(order['order_id']), 'False', e2bRecordBull['Pair Time'][i]]
                                graveyardRecordBull = addRow(graveyardRecordBull, temp)
                        else:
                            e2bRecordBull["Confirmation"][i] = "Fail"
                            temp = e2bRecordBull.iloc[i, :]
                            graveyardRecordBull = addRow(graveyardRecordBull, temp)
                            e2bRecordBull["Delete"][i] = "True"
                            logger(fname, 'e2b bull order status failed after new rate')
                    else:
                        logger(fname, 'canceling e2b bull record for new rate failed')
        else:
            logger(fname, 'e2b record bull order status failed')

    e2bRecordBull = deleteRows(e2bRecordBull)

def sweepBullGraveyard(fname, ethbtc, fastMinuteResults):
    global graveyardRecordBull
    global e2bRecordBull
    global averageBullTradeTime
    global stdBullTradeTime

    for i in range(0, len(graveyardRecordBull)):
        if (graveyardRecordBull['Confirmation'][i] == 'False'):
            rate = Algo.rateLinearDecay(fname, graveyardRecordBull['Rate Needed'][i],
                                         time.time() - graveyardRecordBull['Pair Time'][i], averageBullTradeTime,
                                        averageBullTradeTime + stdBullTradeTime, fastMinuteResults, ethbtc)
            order = Gemcon.newOrder(format(graveyardRecordBull["Amount"][i], '.6f'), format(rate, '.5f'), 'sell', None,
                                    'ethbtc')
            if (order.status_code == 200):
                graveyardRecordBull["ID"][i] = int(order.json()['order_id'])
                e2bRecordBull = addRow(e2bRecordBull, graveyardRecordBull.iloc[[i]])
                graveyardRecordBull["Delete"][i] = "True"
                logger(fname, 'graveyard record bull order resurrected')
            else:
                logger(fname, order.json())
                logger(fname, 'graveyard record bull order failed')
        if (graveyardRecordBull["Confirmation"][i] == "Fail"):
            rate = Algo.rateLinearDecay(fname, graveyardRecordBull['Rate Needed'][i],
                                         time.time() - graveyardRecordBull['Pair Time'][i], averageBullTradeTime,
                                        averageBullTradeTime + stdBullTradeTime, fastMinuteResults, ethbtc)
            temp = Gemcon.orderStatus(graveyardRecordBull["ID"][i])
            if (temp.status_code == 200):
                temp = temp.json()
                amount = float(temp['remaining_amount'])
                if (float(temp['executed_amount']) == 0):
                    order = Gemcon.newOrder(amount, format(rate, '.5f'),
                                            'sell', None, 'ethbtc')
                    graveyardRecordBull["Delete"][i] = "True"
                else:
                    order = Gemcon.newOrder(amount, format(rate, '.5f'), 'sell', None,
                                            'ethbtc')
                if (order.status_code == 200):
                    graveyardRecordBull["Delete"][i] = "True"
                    order = order.json()
                    temp = ["False", graveyardRecordBull['Rate Needed'][i], graveyardRecordBull['Time'][i], amount, rate, int(order['order_id']), 'False', graveyardRecordBull['Pair Time'][i]]
                    e2bRecordBull = addRow(e2bRecordBull, temp)
                else:
                    logger(fname, order.json())
                    logger(fname, 'graveyard [i][1] = 2 order failed')
            else:
                logger(fname, 'graveyard bull order status failed')

        graveyardRecordBull = deleteRows(graveyardRecordBull)

def geminiEthbtc(csvfile):
# Setup variables
#######################################################################################################################
    csv_writer = csv.writer(csvfile)
    print('Starting Gemini Bot')
    global active
    active = True
    ethOffset = float(linecache.getline('settings.cfg', 27).rstrip("\n\r"))
    btcOffset = float(linecache.getline('settings.cfg', 29).rstrip("\n\r"))
    ethReserved = 0
    btcReserved = 0
    ethbtc = 0
    global e2bRecordBear
    global b2eRecordBear
    global graveyardRecordBear
    global b2eRecordBull
    global e2bRecordBull
    global graveyardRecordBull
    global tradeTimeBear
    global tradeTimeBull
    global averageBearTradeTime
    global stdBearTradeTime
    global averageBullTradeTime
    global stdBullTradeTime
    minuteTimeScale = 15
    hourTimeScale = 1
    fastMinuteTimeScale = 5
    e2bRecordBear = pd.DataFrame(
        columns=['Confirmation', 'Reserved', 'Time', 'Amount', 'Rate', 'ID', 'Delete'])
    b2eRecordBear = pd.DataFrame(
        columns=['Confirmation', 'Rate Needed', 'Time', 'Amount', 'Rate', 'ID', 'Delete', 'Pair Time'])
    graveyardRecordBear = b2eRecordBear
    b2eRecordBull = e2bRecordBear
    e2bRecordBull = b2eRecordBear
    graveyardRecordBull = e2bRecordBull
    lastMinuteTime = 0
    lastHourTime = 0
    lastFastMinuteTime = 0
    timeLimit = 900
    global fee
    fee = 0.0025
    averageBearTradeTime = float(linecache.getline('settings.cfg', 17).rstrip("\n\r"))
    stdBearTradeTime = float(linecache.getline('settings.cfg', 19).rstrip("\n\r"))
    tradeTimeBear = np.asarray([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    tradeTimeBull = tradeTimeBear
    tradeTimeBear[-1] = averageBearTradeTime

    averageBullTradeTime = float(linecache.getline('settings.cfg', 21).rstrip("\n\r"))
    stdBullTradeTime = float(linecache.getline('settings.cfg', 23).rstrip("\n\r"))
    tradeTimeBull[-1] = averageBullTradeTime
    global loggingEnable
    loggingEnable = str(linecache.getline('settings.cfg', 31)).rstrip("\n\r")
    fname = 'output_gemini_' + str(int(time.time())) + '.txt'
    if (loggingEnable == 'true'):
        f = open(fname, 'w')
        f.close()
    noHeader = True
########################################################################################################################

    # main loop
    while (active):
        lockout = False
        timeNow = time.time()
        logger(fname, timeNow)

        # update trade time stats
        if (np.count_nonzero(tradeTimeBear) == 0):
            averageBearTradeTime, stdBearTradeTime = norm.fit(tradeTimeBear)
        if (np.count_nonzero(tradeTimeBull) == 0):
            averageBullTradeTime, stdBullTradeTime = norm.fit(tradeTimeBull)

        # get indicator data
        timeScale = hourTimeScale  # in hours
        if (time.time() >= lastHourTime + timeScale * 3600):
            try:
                hourResults = hourData('Gemini', 'ETH', "BTC", str(timeScale), '500')
                logger(fname, 'new ethbtc hour data!')
                lastHourTime = hourResults.loc[0, 'time']
            except Exception as e:
                logger(fname, 'ethbtc hour data try failed')
                logger(fname, e)
                lockout = True
        timeScale = minuteTimeScale  # in minutes
        if (time.time() >= lastMinuteTime + timeScale * 60):
            try:
                minuteResults = minuteData('Gemini', 'ETH', "BTC", str(timeScale), '500')
                lastMinuteTime = minuteResults.loc[0, 'time']
                logger(fname, 'new ethbtc minute data!')
            except Exception as e:
                lockout = True
                logger(fname, 'ethbtc minute data try failed')
                logger(fname, e)
        timeScale = fastMinuteTimeScale  # in minutes
        if (time.time() >= lastFastMinuteTime + timeScale * 60):
            try:
                fastMinuteResults = minuteData('Gemini', 'ETH', "BTC", str(timeScale), '500')
                lastFastMinuteTime = fastMinuteResults.loc[0, 'time']
                logger(fname, 'new ethbtc fast minute data!')
            except Exception as e:
                lockout = True
                logger(fname, 'ethbtc fast minute data try failed')
                logger(fname, e)

        # get current market rate
        try:
            ethbtc = geminiLastPrice()
        except Exception as e:
            lockout = True



        if (lockout == False):
            # write csv header if need be
            if (noHeader):
                minuteResultsHeader = np.array(minuteResults.columns.values.tolist(), dtype=object)
                for i in range(0, minuteResultsHeader.size):
                    minuteResultsHeader[i] = minuteResultsHeader[i] + " " + str(minuteTimeScale) + " " + "minute"
                hourResultsHeader = np.array(hourResults.columns.values.tolist(), dtype=object)
                for i in range(0, hourResultsHeader.size):
                    hourResultsHeader[i] = hourResultsHeader[i] + " " + str(hourTimeScale) + " " + "hour"
                fastMinuteResultsHeader = np.array(fastMinuteResults.columns.values.tolist(), dtype=object)
                for i in range(0, fastMinuteResultsHeader.size):
                    fastMinuteResultsHeader[i] = fastMinuteResultsHeader[i] + " " + str(fastMinuteTimeScale) + " " + "minute"
                header = np.array(["Time", "ETH BTC"], dtype=object)
                header = np.concatenate((header, minuteResultsHeader, hourResultsHeader, fastMinuteResultsHeader))
                csv_writer.writerow(header)
                csvfile.flush()
                noHeader = False

            try:
                # calculate amount of eth locked up in bear trades so they wont be used in bull trades
                ethReserved = e2bRecordBear["Reserved"].sum()
                # core sequence
                e2bSig = Algo.eth2BtcSignalBear(fname, ethbtc, minuteResults, hourResults)
                logger(fname, 'bear e2bsig is ' + str(e2bSig))
                if (e2bSig):
                    e2bBearTradeExecute(fname, btcOffset, ethOffset, ethReserved, ethbtc)
                e2bConfirmCancelOrders(fname, timeLimit)
                e2bBullTradeExecute(fname, ethbtc, fastMinuteResults)
                b2eConfirmCancelUpdateOrders(fname, ethbtc, fastMinuteResults)
                sweepBearGraveyard(fname, ethbtc, fastMinuteResults)
                # write out record to log
                logger(fname, 'e2b record bear:')
                logger(fname, e2bRecordBear)
                logger(fname, 'b2e record bear:')
                logger(fname, b2eRecordBear)
                logger(fname, 'graveyardRecordBear:')
                logger(fname, graveyardRecordBear)
                logger(fname, 'TICK')
                logger(fname, '\n')



                # calculate amount of btc locked up in bear trades so they wont be used in bull trades
                btcReserved = np.sum(e2bRecordBear, 0)[1]
                # core sequence
                b2eSig = Algo.btc2ethSignalBear(fname, ethbtc, minuteResults, hourResults)
                logger(fname, 'bull b2eSig is ' + str(b2eSig))
                if (b2eSig):
                    e2bBearTradeExecute(fname, btcOffset, ethOffset, btcReserved, ethbtc)
                b2eConfirmCancelOrders(fname, timeLimit)
                b2eBearTradeExecute(fname, ethbtc, fastMinuteResults)
                e2bConfirmCancelUpdateOrders(fname, ethbtc, fastMinuteResults)
                sweepBullGraveyard(fname, ethbtc, fastMinuteResults)
                # write out record to log
                logger(fname, 'b2e record bull:')
                logger(fname, b2eRecordBull)
                logger(fname, 'e2b record bull:')
                logger(fname, e2bRecordBull)
                logger(fname, 'graveyard bull:')
                logger(fname, graveyardRecordBull)
                logger(fname, 'TOCK')
                logger(fname, '\n')
            except Exception as e:
                logger(fname, e)
                pass


            time.sleep(5)


            # write to csv
            minuteResultsArray = np.array(minuteResults.iloc[[0]], dtype=object)[0]
            hourResultsArray = np.array(hourResults.iloc[[0]], dtype=object)[0]
            fastMinuteResultsArray = np.array(fastMinuteResults.iloc[[0]], dtype=object)[0]
            row = np.concatenate(( (np.array([timeNow, ethbtc], dtype=object), minuteResultsArray, hourResultsArray, fastMinuteResultsArray)))
            csv_writer.writerow(row)
            csvfile.flush()




        else:
            logger(fname, 'lockout is active')
            



with open('results_' + str(time.time()) + '.csv', 'w+', newline='') as csvfile:
    Gemcon.cancel_all()
    #quit()
    geminiEthbtc(csvfile)
