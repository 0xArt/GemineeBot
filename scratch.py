import APIcalls 
import json
CC = APIcalls.CryptoCompare()


data = json.loads(CC.minute_hist('Binance', 'ETH', '10', '10'))
print(data)
