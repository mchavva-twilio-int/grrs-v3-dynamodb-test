# GRR v3 - Data Model validation

## Setup

- Pre-requisite Installations
    - AWS CLI
    - Local Dynamo db instance.
    - virtual env
        - ```pip3 install virtualenv```

## Data Load

Once you import the [data model](https://docs.google.com/document/d/1Y_kvDxH81NvgqyusNcDx3tFeFK_ZCzn9WXITP9QhbPo/edit) and create the required entities, Prepare some data to perform batch writes. 
- use [mockaroo](https://www.mockaroo.com/) to generate mock data.
- convert it into dynamodb format using [dynamodb format converter](https://dynobase.com/dynamodb-json-converter-tool/).
- Finally prepare a json document to execute a batch-write operation. See samples in the folder `data`



## Run

- Activate virtual env.
    ```
    python3 -m venv <name_of_virtualenv> 
    source <name_of_virtualenv>/bin/activate 
    ```
- Install the required python packages.

    - ``` pip3 install -r requirements.txt```

- ### To run pre-made-scan-test, use
    ``` python3 -W ignore pre-made-scan-vs-get-item-perf.py ```

- ### To run account-routes-query-test, use
    ``` python3 -W ignore acct-routes-query-perf.py ```

