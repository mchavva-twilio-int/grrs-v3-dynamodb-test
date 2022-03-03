import profile
from socket import if_indextoname
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from pprint import pprint
from utils import Utils

import boto3
import time
import pandas as pd
import asyncio
import matplotlib.pyplot as plot
import seaborn as sns
import numpy as np
import random


session = boto3.Session(profile_name='local-db-profile')
db = session.resource('dynamodb', endpoint_url='http://localhost:8000')  

def run_query():
    table = db.Table('account_route_configurations')
    try:
        # sid - 2d6e6c62-e46b-4b9b-a334-61e6d6f89e17
        response = table.query(
            IndexName = 'account_sid',
            KeyConditionExpression = Key('account_sid').eq('AC85349f32db59a8c13d87faefd6dd60dc'),
            FilterExpression= Attr('routes.voice').eq('us1')
        )
        print(response['Items'])
        
                
        # Pretty raw syntax and works just fine!!
        # response = table.scan(
        #     FilterExpression= 'attribute_exists(#0.#1)',
        #     ExpressionAttributeNames = {
        #       "#0" : "routes",
        #       "#1" : "voice"
        #     }
        # )
        
        # response = table.scan(
        #     FilterExpression= '#0.#1 = :v',
        #     ExpressionAttributeNames = {
        #       "#0" : "routes",
        #       "#1" : "voice"
        #     },
        #     ExpressionAttributeValues = {
        #         ":v" : "au1",
        #     }
        # )
    except ClientError as e:
        print(e.response['Error']['Message'])
    

async def run_query_async(max_range: int, result_file: str):
    table = db.Table('account_route_configurations')
    regions = ['us1', 'ie1', 'de1', 'au1', 'jp1']
    df1 = pd.DataFrame(columns=['Query'])
    
    try:
        for i in range(0, max_range):
            start_timer = time.perf_counter()
            response = table.query(
                IndexName = 'account_sid',
                KeyConditionExpression = Key('account_sid').eq('AC85349f32db59a8c13d87faefd6dd60dc'),
                FilterExpression= Attr('routes.voice').eq(random.choice(regions))
            )
            end_timer = time.perf_counter()
            df1 = df1.append({'Query': end_timer-start_timer}, ignore_index=True)   
            # pprint(response['Items'], sort_dicts=False)    
    
        
        print(df1.describe(percentiles=[0.25,0.5,0.75,0.90,0.95, 0.99],include='all'))
    
        df1.to_csv(result_file,index=False)
    except ClientError as e:
        # print(e.response['Error']['Message'])
        raise e

def get_account_route_sids():
    return [
        "8c7bl673-a49b-4e07-9129-9a63b9dd0770",
        "6467l63c-2d12-413e-9e29-b985071abfd6",
        "4bb5l87f-8a95-4879-be59-fb0e82af6eec",
        "66a7lc14-f8cc-40db-a969-586bd788bb0q",
        "fe2al0ec-b3e3-486f-88c9-940dc205cbcq",
        "6719la67-50b0-4234-b1f9-2342242dd39q",
        "b5bel9ce-9a9c-4d25-a1a9-b3b178a674bq",
        "6cf8l061-8904-4cf6-bcc9-034a348b931q",
        "03a3lc04-99a3-4d84-ac59-a03055bd778q",
        "2f80ldcb-6d40-44ac-9749-3ca976d915fq",
        "0c6ela34-bf32-48f8-b5f9-90e5af286afq",
        "ac3alcc6-861c-4d65-a039-b7034466d44q",
        "f4b2lbf6-cc87-4509-8cf9-49d2c358667q",
        "6f54lbfb-8661-4f64-abc9-6599d46b46bq",
        "d018ld37-56c0-4b9c-b0c9-e52992fcb60q",
        "fbffl040-5329-40f0-bf59-0f86f908ca5q",
        "e5b7l304-4b2b-4f37-a159-a7892e808d7q",
        "3b48b52d-c780-4be0-ab9c-fcbdf25d3f83",
        "fdb8a1f8-1fcb-416c-8cc4-f61d40da9e48"
    ]

async def run_query_vs_get_item_async(max_range: int, result_file: str):
    table = db.Table('account_route_configurations')
    regions = ['us1', 'ie1', 'de1', 'au1', 'jp1']
    df1 = pd.DataFrame(columns=['Query'])
    df2 = pd.DataFrame(columns=['GetItem'])
    
    sids = get_account_route_sids()
    
    try:
        
        # Execute the query operation
        for i in range(0, max_range):
            start_timer = time.perf_counter()
            response = table.query(
                IndexName = 'account_sid',
                KeyConditionExpression = Key('account_sid').eq('AC85349f32db59a8c13d87faefd6dd60dc'),
                FilterExpression= Attr('routes.voice').eq(random.choice(regions))
            )
            end_timer = time.perf_counter()
            df1 = df1.append({'Query': end_timer-start_timer}, ignore_index=True)   
            # pprint(response['Items'], sort_dicts=False)    
        
        # Execute the get-item operation.
        for i in range(0, max_range):
            start_timer1 = time.perf_counter()
            response = table.get_item(
                Key={'sid': random.choice(sids)}
            )
            end_timer1 = time.perf_counter()
            df2 = df2.append({'GetItem': end_timer1-start_timer1}, ignore_index=True)
        
        df_merged = pd.concat([df1, df2], axis=1)
        print(df_merged.describe(percentiles=[0.25,0.5,0.75,0.90,0.95, 0.99],include='all'))
    
        df_merged.to_csv(result_file,index=False)
    except ClientError as e:
        # print(e.response['Error']['Message'])
        raise e
    
def execute_event_loop(result_file):
    # Define the max range value of operations to be executed.
    MAX_ITERATIONS = 200
    loop = asyncio.get_event_loop()
    # loop.run_until_complete(run_query_async(max_range=MAX_ITERATIONS, result_file=result_file))
    loop.run_until_complete(run_query_vs_get_item_async(max_range=MAX_ITERATIONS, result_file=result_file))
    loop.close()


def main():
    #run_query()
    
    #RESULT_FILE = './data/account-routes-query-result.csv'
    #STATS_FILE = './data/stats/account-routes-query-result.png'
    
    RESULT_FILE = './data/account-routes-query-vs-get-item-result.csv'
    STATS_FILE = './data/stats/account-routes-query-vs-get-item-result.png'
    
    print("\nThis is a test on dynamo db query vs get-item")
    execute_event_loop(result_file=RESULT_FILE)
    
    print("\n\nGenerating stats graph for the test results.. ")
    Utils.generate_stats_graph(result_file=RESULT_FILE, stats_file=STATS_FILE)

if __name__ == "__main__":
    main()