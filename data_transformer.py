import boto3
import json
import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", "/tmp", 'yfinance'])
sys.path.append('/tmp')

import yfinance as yf

def lambda_handler(event, context):
    
    # initialize boto3 client
    kinesis = boto3.client("kinesis", "us-east-2")
    
    #get data by yfinance.history
    stock_symbol = ['FB','SHOP','BYND','NFLX','PINS','SQ','TTD','OKTA','SNAP','DDOG']
    data = yf.Tickers(stock_symbol)
    history = data.history(start="2020-12-01", end="2020-12-02",interval="5m",group_by = 'tickers')

    req = []

    for stock in stock_symbol:
        for index, value in history[stock].iterrows():
            dataf = {'high':value['High'],'low':value['Low'],'ts':index.strftime('%Y-%m-%d %H:%M:%S'),'name':stock}
            
            #convert data into JSON
            as_jsonstr = json.dumps(dataf)+"\n"
            
            # push data to kinesis
            kinesis.put_record(
                StreamName="STA9760F2020_stream1",
                # convert datatypeinto bytes
                Data=as_jsonstr.encode('utf-8'),
                PartitionKey="partitionkey")
            
            req.append(dataf)

    # return
    return {
        'statusCode': 200,
        'body': json.dumps(f'Done! Data: {as_jsonstr}')
    }
