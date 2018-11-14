import numpy as np
import linecache
import pandas as pd


class TradeAlgo():
    global loggingEnable
    loggingEnable = str(linecache.getline('settings.cfg', 31)).rstrip("\n\r")

    def logger(self,fname, text, printer=True):
        if(loggingEnable == 'true'):
            f = open(fname, 'a')
            if(printer == True):
                print(text)
            f.write(str(text))
            f.write('\n')
            f.flush()
            f.close()

    def roundDown(self, n, d) -> np.float:
        d = int('1' + ('0' * d))
        return np.floor(n * d) / d

    def rate_linear_growth(self, fname, rn, past_time, start, end, ema5, ema10, ethbtc) -> float:
        min = rn - (6e-4)
        rate = min
        self.logger(fname, 'past time for GROWTH is: ' + str(past_time))
        self.logger(fname, 'start is: ' + str(start))
        self.logger(fname, 'end is: ' + str(end))
        if (past_time >= start and past_time < end):
            m = (rn - min) / ((end - start))
            rate = m * (past_time - start) + min
            rate = self.roundDown(rate, 5)
        if (past_time >= end):
            rate = rn
        if(ema5 / ema10 >= 1.03):
            rate = rn
            self.logger(fname, 'ema3 clause tripped')
        if( ethbtc[-1] < rate):
            self.logger(fname, 'b2e bear rate2 is: ' + str(ethbtc[-1]))
            self.logger(fname, 'b2e bear rn is: ' + str(rn))
            return self.roundDown(ethbtc[-1], 5)
        else:
            self.logger(fname, 'b2e bear rate2 is: ' + str(rate))
            self.logger(fname, 'b2e bear rn is: ' + str(rn))
            return rate

    def rateLinearGrowth(self, fname, rn, past_time, timeArray, minute3Results, ethbtc) -> float:
        ema5 = minute3Results.loc[:, 'ema5']
        ema10 = minute3Results.loc[:, 'ema10']
        start = timeArray[0]
        end = timeArray[1]
        min = rn - (6e-4)
        rate = min
        self.logger(fname, 'past time for GROWTH is: ' + str(past_time))
        self.logger(fname, 'start is: ' + str(start))
        self.logger(fname, 'end is: ' + str(end))
        if (past_time >= start and past_time < end):
            m = (rn - min) / ((end - start))
            rate = m * (past_time - start) + min
            rate = self.roundDown(rate, 5)
        if (past_time >= end):
            rate = rn
        if(ema5 / ema10 >= 1.03):
            rate = rn
            self.logger(fname, 'ema3 clause tripped')
        if( ethbtc[-1] < rate):
            self.logger(fname, 'b2e bear rate2 is: ' + str(ethbtc[-1]))
            self.logger(fname, 'b2e bear rn is: ' + str(rn))
            return self.roundDown(ethbtc[-1], 5)
        else:
            self.logger(fname, 'b2e bear rate2 is: ' + str(rate))
            self.logger(fname, 'b2e bear rn is: ' + str(rn))
            return rate

    def rate_linear_decay(self, fname, rn, past_time, start, end, ema5, ema10, ethbtc) -> float:
        max = rn + (6e-4)
        rate = max
        self.logger(fname, 'past time for DECAY is: ' + str(past_time))
        self.logger(fname, 'start is: ' + str(start))
        self.logger(fname, 'end is: ' + str(end))
        if (past_time >= start and past_time < end):
            m = (rn - max) / ((end - start))
            rate = m * (past_time - start) + max
            rate = np.round(rate, 5)
        if (past_time >= end):
            rate = rn
        if(ema5 / ema10 <= 0.97):
            rate = rn
            self.logger(fname, 'ema3 clause tripped')
        if( ethbtc[-1] > rate):
            self.logger(fname, 'e2b bull rate2 is: ' + str(ethbtc[-1]))
            self.logger(fname, 'e2b  bull rn is: ' + str(rn))
            return np.round(ethbtc[-1],5)
        else:
            self.logger(fname, 'e2b bull rate2 is: ' + str(rate))
            self.logger(fname, 'e2b bull rn is: ' + str(rn))
            return rate


    def eth2btc_signal_15m_bear(self, fname, ethbtc, minuteResults, hourResults) -> bool:
        ema5 = minuteResults.loc[:, 'ema5']
        ema10 = minuteResults.loc[:, 'ema10']
        diMinus = minuteResults.loc[:, 'di-']
        diPlus = minuteResults.loc[:, 'di+']
        w14m = minuteResults.loc[:, 'w14']
        adx = minuteResults.loc[:, 'adx']
        w14h = hourResults.loc[:, 'w14']
        self.logger(fname, 'eth btc is: ' + str(ethbtc[-1]))
        pdif = (diMinus - diPlus) / (diPlus)
        if (ema5 <= ema10 and w14m > -90 and w14h < -45 and w14h > -85 and adx > 25 and pdif > 0.06):
            return True
        else:
            self.logger(fname, 'failed 15m bear core test')
            # print("eth2btc signal false")
            return False


    def rateNeededBear(self, amount, ethbtc) -> float:
        sig = 0
        fee = 0.0025
        btc = amount * ethbtc - amount * ethbtc * fee
        btc = btc - btc * fee
        rn = (btc / amount) - 0.00001
        rn = self.roundDown(rn, 5)
        return rn

    def btc2ethSignalWithGrowthBear(self, fname, ethbtc, amount, rate, pastTime, minuteResults, tradeTimeArray) -> bool:
        ema5 = minuteResults.loc[:, 'ema5']
        ema10 = minuteResults.loc[:, 'ema10']
        sig = 0
        fee = 0.0025
        btc = amount * ethbtc - amount * ethbtc * fee
        btc = btc - btc * fee
        rn = self.rateNeededBear(amount, ethbtc)
        #rate2 = self.rate_linear_growth(fname, rn, pastTime, start, end, ema5_2, ema10_2, ethbtc)
        if( ((ema5>ema10) and (ethbtc[-1]<rn)) or ((ethbtc-ethbtc[-1])>6e-4)):
            return True;
        return False;


    def btc2eth_signal_15m_bull(self, fname, ethbtc, minResults, hourResults) -> bool:
        ema5 = minResults.loc[:, 'ema5']
        ema10 = minResults.loc[:, 'ema10']
        diMinus = minResults.loc[:, 'di-']
        diPlus = minResults.loc[:, 'di+']
        w14m = minResults.loc[:, 'w14']
        adx = minResults.loc[:, 'adx']
        w14h = hourResults.loc[:, 'w14']
        self.logger(fname, 'eth btc is: ' + str(ethbtc[-1]))
        pdif = (diMinus - diPlus) / (diPlus)

        if (ema5 >= ema10 and w14m < -10 and w14h < -15 and w14h > -55 and adx > 25 and pdif < -0.08):
            return True
        else:
            self.logger(fname, 'failed 15m bull core test')
            # print("eth2btc signal false")
            return False

    def eth2btc_signal_with_decay_bull(self, fname, ethbtc, ema5, ema10, amount, rate, past_time, ema5_2, ema10_2, start, end) -> np.array:
        # amount in ETH
        rn=0
        sig=0
        fee=0.0025
        cost=(rate * amount) * (1 + fee)
        if (cost != 0):
            rn=((cost * (1 + fee)) / (amount)) + 0.00001
            rn=np.round(rn, 5)
            rate2=self.rate_linear_decay(fname, rn, past_time, start, end, ema5_2, ema10_2, ethbtc)
            if((ema5 < ema10 and ethbtc[-1] > rn) or (ethbtc[-1] - rate > 6e-4) or (ethbtc[-1] >= rate2)):
                sig=1
        return np.array([sig, rn])
