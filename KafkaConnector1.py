import confluent_kafka
from random import randint
import time
import requests
import  pandas as pd

from confluent_kafka import Consumer
from confluent_kafka import Producer
import sched, time
import asyncio  

def delivery_report(err, msg):
    if err:
        if str(type(err).__name__) == "KafkaError":
            print(f"Message failed with error : {str(err)}")
            print(f"Message Retry? :: {err.retriable()}")
        else:
            print(f"Message failed with error : {str(err)}")
    else:
        print(f"Message delivered to partition {msg.partition()}; Offset Value - {msg.offset()}")
        print(f"{msg.value()}")

def run_producer():
    p = Producer({'bootstrap.servers':'pkc-4r297.europe-west1.gcp.confluent.cloud:9092',
                  'security.protocol':'sasl_ssl',
                  'sasl.mechanism':'PLAIN',
                  'sasl.username':'J7JQLVI43NGDTJRD',
                  'sasl.password':'agWoZRv/8tDejLpx0gH9FZFskys6SrT2wWTGoU+K8g4AYASUC0sqR6fTu3a92hpz',
                  'acks':'-1',
                  'partitioner':'consistent_random',
                  'batch.num.messages':'1000',
                  'linger.ms':'100',
                  'queue.buffering.max.messages':'10000'})
    return p

       



def write_to_queue(p,lat, lon):
    while True:
            try:
                response = requests.get("https://api.openweathermap.org/data/2.5/onecall?lat="+str(lat)+"&lon="+str(lon)+"&exclude=hourly,daily&appid=0170303693f51ac954ab9e1f360d2dc9")
                print(response.json())
                if (response.json!=401) & (response!=400):
                    msg_value=response.json()
                    msg_header = {"source" : b'DEM'}
                    p.poll(timeout=0)
                    p.produce(topic='weather', value=str(msg_value), headers=msg_header, on_delivery=delivery_report)
                break
            except BufferError as buffer_error:
                    print(f"{buffer_error} :: Waiting until Queue gets some free space")
                    time.sleep(1)
    p.flush()
    
async def cr_Germany(p):
    while True:
        await asyncio.sleep(1)
        write_to_queue(p, 51.1657, 10.4515)       
        print("cr_Germany Worker Executed")
        
async def cr_India(p):
    while True:
        await asyncio.sleep(1)
        write_to_queue(p, 20.593, 78.9629)       
        print("cr_India Worker Executed")
        
async def cr_USA(p):
    while True:
        await asyncio.sleep(1)
        write_to_queue(p, 37.0902, 95.712)       
        print("cr_USA Executed")
    
async def cr_UAE(p):
    while True:
        await asyncio.sleep(1)
        write_to_queue(p, 56.1304, 53.8478)       
        print("cr_UAE Task Executed")
        
async def cr_Canada(p):
    while True:
        await asyncio.sleep(1)
        write_to_queue(p, 56.1304, 106.3468)       
        print("cr_Canada Task Executed")
        
async def cr_Sweden(p):
    while True:
        await asyncio.sleep(1)
        write_to_queue(p, 60.1282, 18.6435)       
        print("cr_Sweden Task Executed")
        
async def cr_Indonesia(p):
    while True:
        await asyncio.sleep(1)
        write_to_queue(p, 0.7893, 113.9213)       
        print("cr_Indonesia Task Executed")
        
async def cr_Zambia(p):
    while True:
        await asyncio.sleep(1)
        write_to_queue(p, 13.1339, 27.8493)       
        print("cr_Zambia Task Executed")
        
async def cr_Australia(p):
    while True:
        await asyncio.sleep(1)
        write_to_queue(p, 25.2744, 133.7751)       
        print(" cr_Australia Task Executed")
        
async def cr_Russian(p):
    while True:
        await asyncio.sleep(1)
        write_to_queue(p, 61.524, 105.3188)       
        print("cr_Russian Task Executed")
        
  

 
   
loop = asyncio.get_event_loop()
try:
    if __name__ == "__main__":
       p=run_producer()
       asyncio.ensure_future(cr_Germany(p))
       asyncio.ensure_future(cr_India(p))
       asyncio.ensure_future(cr_Australia(p))
       asyncio.ensure_future(cr_Canada(p))
       asyncio.ensure_future(cr_Indonesia(p))
       asyncio.ensure_future(cr_Russian(p))
       asyncio.ensure_future(cr_Sweden(p))
       asyncio.ensure_future(cr_UAE(p))
       asyncio.ensure_future(cr_USA(p))
       asyncio.ensure_future(cr_Zambia(p))
       loop.run_forever()
except KeyboardInterrupt:
    pass
finally:
    print("Closing Loop")
    loop.close()


