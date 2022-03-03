import profile
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr
from pprint import pprint

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


async def run_scan_async(max_range: int, result_file: str):
    table = db.Table('pre_made_route_configurations')
    regions = ['us1', 'ie1', 'de1', 'au1', 'jp1']
    df1 = pd.DataFrame(columns=['Scan'])
    
    try:
        for i in range(0, max_range):
            start_timer = time.perf_counter()
            response = table.scan(
                FilterExpression= Attr('routes.voice').eq(random.choice(regions)),
                # Attr('routes.voice').is_in(value=['us1', 'au1']),
                # Attr('routes.voice').exists()
            )
            end_timer = time.perf_counter()
            df1 = df1.append({'Scan': end_timer-start_timer}, ignore_index=True)   
            # pprint(response['Items'], sort_dicts=False)    
    
        
        print(df1.describe(percentiles=[0.25,0.5,0.75,0.90,0.95, 0.99],include='all'))
    
        df1.to_csv(result_file,index=False)
        
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
        # print(e.response['Error']['Message'])
        raise e
    

def run_get_item():
    table = db.Table('pre_made_route_configurations')
    try:
        response = table.get_item(Key={'sid': '90fe99da-799d-431b-a676-605aee298ea8'})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']
    
async def run_scan_vs_get_item_test_async(max_range: int, result_file: str):
    table = db.Table('pre_made_route_configurations')
    regions = ['us1', 'ie1', 'de1', 'au1', 'jp1']
    region_sids = [
        '90fe99da-799d-431b-a676-605aee298ea8',
        '25d36e23-0bf5-4e18-9b51-f2b215019a4e',
        'ed3e1747-8f01-46a9-b094-aa89e257a350',
        'e483395e-05b5-4ff3-afab-9e2543c0ed92',
        '131b8d5c-470a-4b8c-97cb-d81941fc8023'
    ]
    df1 = pd.DataFrame(columns=['Scan'])
    df2 = pd.DataFrame(columns=['GetItem'])
    
    try:
        # Execute the scan operation.
        for i in range(0, max_range):
            start_timer = time.perf_counter()
            response = table.scan(
                FilterExpression= Attr('routes.voice').eq(random.choice(regions)),
            )
            end_timer = time.perf_counter()
            df1 = df1.append({'Scan': end_timer-start_timer}, ignore_index=True)
    
    
        # Execute the get-item operation.
        for i in range(0, max_range):
            start_timer1 = time.perf_counter()
            response = table.get_item(
                Key={'sid': random.choice(region_sids)}
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
    loop.run_until_complete(run_scan_vs_get_item_test_async(max_range=MAX_ITERATIONS, result_file=result_file))
    loop.close()

def generate_stats_graph(result_file, stats_file):

    df = pd.read_csv(result_file)
    
        
    fig, axes = plot.subplots(1, 2, figsize=(12, 5), sharey=False)
    
    #generate response time distribution
    kwargs = dict(element='step',shrink=.8, alpha=0.6, fill=True, legend=True) 
    ax = sns.histplot(ax=axes[0],data=df,**kwargs)
    #ax.set(xlim=(0.00,1.00)) #set the ylim boundary if auto option is not what you need
    ax.set_title('Response Time Distribution')
    ax.set_xlabel('Response Time (s)')
    ax.set_ylabel('Request Count')
    
    
    #generate percentile distribution       
    summary = np.round(df.describe(percentiles=[0.0, 0.1, 0.2,
                                                     0.3, 0.4, 0.5,
                                                     0.6, 0.7, 0.8,  
                                                     0.9, 0.95, 0.99, 1]),2) # add 1 in the percentile
    dropping = ['count', 'mean', 'std', 'min','max'] #remove metrics not needed for percentile graph
    
    for drop in dropping:
        summary = summary.drop(drop)
    ax = sns.lineplot(ax=axes[1],data=summary,dashes=False, legend=True)
    ax.legend(fontsize='medium')
    #ax.set(ylim=(0.0,1.0)) #set the ylim boundary if auto option is not what you need
    ax.set_title('Percentile Distribution')
    ax.set_xlabel('Percentile')
    ax.set_ylabel('Response Time (s)')
    
    fig.tight_layout(pad=1)
    plot.savefig(stats_file)

def main():
    RESULT_FILE = './data/pre-made-scan-vs-get-item-result.csv'
    STATS_FILE = './data/stats/pre-made-scan-vs-get-item-result.png'
    
    print("\nThis is a test on dynamo db scan vs get-item for tiny tables")
    execute_event_loop(result_file=RESULT_FILE)
    
    print("\n\nGenerating the stats graph for the results.. ")
    generate_stats_graph(result_file=RESULT_FILE, stats_file=STATS_FILE)
    

if __name__ == "__main__":
    main()