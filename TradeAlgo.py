import numpy as np
import linecache
import pandas as pd


class TradeAlgo():
    global loggingEnable
    loggingEnable = str(linecache.getline('settings.cfg', 31)).rstrip("\n\r")
    global fee
    fee = 0.0025

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

    def rateLinearGrowth(self, fname, rn, past_time, start, end, fastMinuteResults, ethbtc) -> float:
        ema5 = fastMinuteResults.loc[0, 'ema5']
        ema10 = fastMinuteResults.loc[0, 'ema10']
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
        if( ethbtc < rate):
            self.logger(fname, 'b2e bear rate2 is: ' + str(ethbtc))
            self.logger(fname, 'b2e bear rn is: ' + str(rn))
            return self.roundDown(ethbtc, 5)
        else:
            self.logger(fname, 'b2e bear rate2 is: ' + str(rate))
            self.logger(fname, 'b2e bear rn is: ' + str(rn))
            return rate


    def eth2BtcSignalBear(self, fname, ethbtc, minuteResults, hourResults) -> bool:
        ema5 = minuteResults.loc[0]['ema5']
        ema10 = minuteResults.loc[0]['ema10']
        diMinus = minuteResults.loc[0]['di-']
        diPlus = minuteResults.loc[0]['di+']
        w14m = minuteResults.loc[0]['w14']
        adx = minuteResults.loc[0]['adx']
        w14h = hourResults.loc[0]['w14']
        self.logger(fname, 'eth btc is: ' + str(ethbtc))
        pdif = (diMinus - diPlus) / (diPlus)
        if (ema5 <= ema10 and w14m > -90 and w14h < -45 and w14h > -85 and adx > 25 and pdif > 0.06):
            return True
        else:
            self.logger(fname, 'failed 15m bear core test')
            # print("eth2btc signal false")
            return False


    def rateNeededBear(self, amount, rate) -> float:
        global fee
        btc = amount * rate - amount * rate * fee
        btc = btc - btc * fee
        rn = (btc / amount) - 0.00001
        rn = self.roundDown(rn, 5)
        return rn

    def btc2ethSignalWithGrowthBear(self, fname, ethbtc, amount, rate, pastTime, fastMinuteResults, start, end) -> bool:
        ema5 = fastMinuteResults.loc[0, 'ema5']
        ema10 = fastMinuteResults.loc[0, 'ema10']
        rn = self.rateNeededBear(amount, rate)
        rate2 = self.rateLinearGrowth(fname, rn, pastTime, start, end, fastMinuteResults, ethbtc)
        if( ((ema5>ema10) and (ethbtc<rn)) or ((ethbtc-ethbtc)>6e-4) or ethbtc<=rate2):
            return True
        return False


    def btc2ethSignalBear(self, fname, ethbtc, minResults, hourResults) -> bool:
        ema5 = minResults.loc[0, 'ema5']
        ema10 = minResults.loc[0, 'ema10']
        diMinus = minResults.loc[0, 'di-']
        diPlus = minResults.loc[0, 'di+']
        w14m = minResults.loc[0, 'w14']
        adx = minResults.loc[0, 'adx']
        w14h = hourResults.loc[0, 'w14']
        self.logger(fname, 'eth btc is: ' + str(ethbtc))
        pdif = (diMinus - diPlus) / (diPlus)

        if (ema5 >= ema10 and w14m < -10 and w14h < -15 and w14h > -55 and adx > 25 and pdif < -0.08):
            return True
        else:
            self.logger(fname, 'failed 15m bull core test')
            # print("eth2btc signal false")
        return False


    def rateLinearDecay(self, fname, rn, pastTime, start, end, fastMinuteResults, ethbtc) -> float:
        ema5 = fastMinuteResults.loc[0, 'ema5']
        ema10 = fastMinuteResults.loc[0, 'ema10']
        max = rn + (6e-4)
        rate = max
        self.logger(fname, 'past time for DECAY is: ' + str(pastTime))
        self.logger(fname, 'start is: ' + str(start))
        self.logger(fname, 'end is: ' + str(end))
        if (pastTime >= start and pastTime < end):
            m = (rn - max) / ((end - start))
            rate = m * (pastTime - start) + max
            rate = np.round(rate, 5)
        if (pastTime >= end):
            rate = rn
        if(ema5 / ema10 <= 0.97):
            rate = rn
            self.logger(fname, 'ema3 clause tripped')
        if( ethbtc > rate):
            self.logger(fname, 'e2b bull rate2 is: ' + str(ethbtc))
            self.logger(fname, 'e2b  bull rn is: ' + str(rn))
            return np.round(ethbtc,5)
        else:
            self.logger(fname, 'e2b bull rate2 is: ' + str(rate))
            self.logger(fname, 'e2b bull rn is: ' + str(rn))
            return rate

    def rateNeededBull(self, amount, rate) -> float:
        global fee
        cost=(rate * amount) * (1 + fee)
        rn=((cost * (1 + fee)) / (amount)) + 0.00001
        rn=np.round(rn, 5)
        return rn

    def eth2btcSignalWithDecayBull(self, fname, ethbtc, amount, rate, pastTime, fastMinuteResults, start, end) -> bool:
        ema5 = fastMinuteResults.loc[0, 'ema5']
        ema10 = fastMinuteResults.loc[0, 'ema10']
        rn = self.rateNeededBull(amount, rate)
        rate2 = self.rateLinearDecay(fname, rn, pastTime, start, end, fastMinuteResults, ethbtc)
        if ((ema5 < ema10 and ethbtc > rn) or (ethbtc - rate > 6e-4) or (ethbtc >= rate2)):
            return True
        return False
