import azure.functions as func
import json
from random import randint
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData
import os
import logging

os.environ["EVENTHUB_CONNECTION_STR"] = "Endpoint=sb://evhloadtest.servicebus.windows.net/;SharedAccessKeyName=streamloadtest_evhloadtest_policy;SharedAccessKey=Hs0mNRkWPVfYfAcARhyhawfT5ye+sYE37+AEhMXci/8=;EntityPath=evhloadtest"
os.environ["EVENTHUB_NAME"] = "evhloadtest"

# Event Hub 연결 문자열 가져오기 
EVENTHUB_CONNECTION_STR = os.environ["EVENTHUB_CONNECTION_STR"]
EVENTHUB_NAME = os.environ["EVENTHUB_NAME"] 

# Function App 구성
app = func.FunctionApp()



# HTTP 트리거 함수 정의
@app.function_name("send_eh_data")  
@app.route(route="send_eh_data")
def main(req: func.HttpRequest) -> func.HttpResponse:

    total_seconds = 60 * 10 
    send_interval = 1

    client = EventHubProducerClient.from_connection_string(conn_str=EVENTHUB_CONNECTION_STR, eventhub_name=EVENTHUB_NAME)

    start_time = datetime.now()  
    while True:
        now = datetime.now()  
        if (now - start_time).seconds >= total_seconds:
            break
            
        data = generate_data()
        event_data_batch = client.create_batch()
        event_data_batch.add(EventData(data))
        
        client.send_batch(event_data_batch)

        time.sleep(send_interval) 
        logging.info('Sent Sample Data')
        
    return func.HttpResponse(status_code=200)

def generate_data():
    text = "key1: " + str(randint(0,1000)).zfill(10 * 1024 * 1024)
    return text