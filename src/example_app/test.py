import pymysql as p
import os
import requests
from dotenv import load_dotenv
import time
import requests
load_dotenv()

host = os.environ.get('RDS_URL')
user = os.environ.get('RDS_USER')
password = os.environ.get('RDS_PASS')
database = os.environ.get('RDS_DB')
#port = int(os.environ.get('RDS_PORT'))
key=os.environ.get('API_KEY')
url=os.environ.get('URL')
device_type = os.environ.get('DEVICE_TYPE')

con=p.connect(host=host,user=user,password=password,database=database)
cur=con.cursor()
def get_device(device_id,site):
    con.ping(reconnect = True)
    query = "select temp_range from devices where deviceId={} and siteId={} and deviceTypeId={}".format(device_id,site,device_type)
    cur.execute(query)
    res = cur.fetchone()
    return res
def get_latlng(site):
    con.ping(reconnect = True)
    query = "select latitude,longitude from sites where siteId={}".format(site)
    cur.execute(query)
    res = cur.fetchone()
    return res    

def get_schedule(device_id,site):
    con.ping(reconnect = True)
    query = "select start_time,end_time,asset_id from schedule where device_id={} and site_id={} and device_type={}".format(device_id,site,device_type)
    cur.execute(query)
    res = cur.fetchall()
    return res

def get_asset(asset_id):
    con.ping(reconnect = True)
    query = "select asset from assets where id={}".format(asset_id)
    print("assetid:", asset_id)
    cur.execute(query)
    res = cur.fetchone()[0]
    return res



def get_temp(lat,lng,rng):
    con.ping(reconnect = True)
    day = time.strftime("%p")
    c =int(float(requests.get(url.format(lat,lng,key)).json()['current']['temp'])-273.15)
    
    if 1 <= c <= rng:
        a = 'R1'
        
    elif rng+1 <= c <= rng * 2:
        a = 'R2'
        
    else:
        a = 'R3'
    
    return day+a

def get_rule(rule):
    con.ping(reconnect = True)
    query = "select id from rules where name='{}'".format(rule)
    cur.execute(query)
    res = cur.fetchone()[0]
    print("rule:",res)
    return res    



def get_assetId(device_id,rule_id,site):
    con.ping(reconnect = True)
    query = "select asset from rule_engine where device_id={} and rule={} and device_type='{}' and site_id={}".format(device_id,rule_id,device_type,site)
    cur.execute(query)
    res = cur.fetchone()[0]
    print("assetid:",res)
    return res        

