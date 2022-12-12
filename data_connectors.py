from abc import ABC, abstractmethod
import pandas as pd
from power_users import config as c
from power_users import google_cloud_storage_utils as csu
from google.cloud import storage
import requests
import json

class DataConnector(ABC):
    @abstractmethod
    def getData(self) -> pd.DataFrame:
        pass

class MetronomoTXCloudStorageConnector(DataConnector):

    # this variabel stores some Metronomo.xyz Google Cloud Storage blobs structure to generalize access
    # to possible different datasets

    ENTITIES = {
        "transactions": {
            "fields": ["signer_account_id", "receiver_account_id", "converted_into_receipt_id"],
        },
        "actions": {
            "fields": ["receipt_id", "action_kind"],
        }
    }

    BLOB_PATHS = {
        "mainnet": {
            "daily": {
                "transactions": "mainnet/daily_data/transactions/",
                "actions": "mainnet/daily_data/action_receipt_actions/"
            }
        }
    }

    # This class implements DataConnector class
    # and incapsulates the logic to retrieve TX data from
    # Metronomno.xyz Google Cloud Storage

    # Current implementation will NOT be useful for anyone who wants to use power_users tool
    # However if you use data from your own Google Cloud Storage it is possible to adjust the code to your needs

    # The main source of data below was NEAR Indexer for Explorer PostgresSQL database
    # https://github.com/near/near-indexer-for-explorer
    # We get transactions table once a day for previous day in a hourly batches.
    # Then we combine hourly batches into one daily batch

    # MetronomoTXCloudStorageConnector stores some data structure for Metronomno.xyz Google Cloud Storage data
    # for using in Power Users module

    def __init__(self,
                 dates,
                 run_local,
                 bucket_name=c.MetronomoTXCloudStorageConnector_DEFAULT_BUCKET_NAME,
                 token_json_path=c.MetronomoTXCloudStorageConnector_TOKEN_JSON_PATH,
                 network=c.MetronomoTXCloudStorageConnector_DEFAULT_NETWORK,
                 granularity=c.MetronomoTXCloudStorageConnector_DEFAULT_GRANULARITY):
        """
        Parameters
        ----------
        dates: str
            dates range to retrieve the data. Should be iterable of datetime.date type
        run_local: str
            flag to run code locally (priority higher than token_json_path). In case of local running path for local toke_json file is used
        bucket_name: str
            name of the bucket to get data from. Either provided or got from config.py file, variable MetronomoTXCloudStorageConnector_DEFAULT_BUCKET_NAME
        token_json_path: str
            path to token json file. Either provided or got from config.py file, variable MetronomoTXCloudStorageConnector_TOKEN_JSON_PATH
        network:
            network to get data from. Currently, possible only "mainnet" data
        granularity:
            data granularity to retrive. Currently possible only "daily" data
        """

        if (run_local):
            self.token_json_path = c.MetronomoTXCloudStorageConnector_LOCAL_TOKEN_JSON_PATH
        else:
            self.token_json_path = token_json_path

        self.storage_client = storage.Client.from_service_account_json(self.token_json_path)
        self.bucket_name = bucket_name
        self.bucket = self.storage_client.get_bucket(self.bucket_name)

        if not (network  in self.BLOB_PATHS.keys()):
            raise ValueError("Wrong network provideded : " + network + ". Network not in BLOB_PATHS. Please choose correct network : " + ", ".join(map(str, self.BLOB_PATHS.keys())))
        if not (network in set(map(lambda x: x.name.split("/")[0], list(self.bucket.list_blobs())))):
            raise ValueError("Wrong network provideded : " + network + ". Network not in bucket")

        self.network = network
        self.dates = dates
        self.granularity = granularity

    def __str__(self):
        return "CloudStorageConnectror object : " + "\n" + \
               "bucket_name : " + str(self.bucket_name) + "\n" + \
               "token_json_path : " + str(self.token_json_path) + "\n" + \
               "storage_client : " + str(self.storage_client) + "\n" + \
               "bucket : " + str(self.bucket) + "\n" + \
               "network : " + str(self.network) + "\n" + \
               "dates : " + str(self.dates) + "\n" + \
               "granularity : " + str(self.granularity)

    def getData(self):
        all_blobs = csu.get_blob_list(self.storage_client, self.bucket)
        tx_blobs = csu.filter_blobs_by_path(
            all_blobs,
            self.BLOB_PATHS[self.network][self.granularity]["transactions"],
        )
        tx_blobs = csu.filter_blobs_by_dates(tx_blobs, self.dates)
        print("tx_blobs : ")
        print(tx_blobs)

        tx_df = pd.DataFrame()
        for tx_b in tx_blobs:
            print("current blob : ")
            print(tx_b)
            tx_data = csu.get_dataframe_from_blob(
                self.bucket,
                tx_b,
                self.ENTITIES["transactions"]["fields"],
                self.token_json_path
            )
            tx_data = tx_data[["signer_account_id", "receiver_account_id", "converted_into_receipt_id"]]
            tx_df = pd.concat([tx_df, tx_data])
        print("tx_df head : ")
        print(tx_df.head(5))
        return tx_df

class MintbaseNFTActivitiesConnector(DataConnector):
    """
    This class implements DataConnector class
    and incapsulates the logic to retrieve TX data from
    Mintbase GraphQL https://docs.mintbase.io/dev/read-data/mintbase-graph

    This class might be usefull to retrieve NFT Activity data for anyone who have list of receipt_ids of interested TX

    Currently you need to have a list of receipt_ids of interested TX to be able to get TX sender and, therefore,
    use this knowledge to find nft_smart_contract power users

    """

    def getData(self, recipies):

        """
        Method to get the NFT Activities data from Mintbase GraphQL https://docs.mintbase.io/dev/read-data/mintbase-graph

        Parameters
        ----------
        recipies : list[str]
            list of recepies for NFT Actions data retrieval (to use in "where: {receipt_id: {_in: $recepies}}" part of query)

        Returns
        ------
        data: pandas.DataFrame
            NFT Activities data
            receipt_id,
            price,
            kind,
            token_id,
            nft_contract_id,
            tx_sender,
            action_receiver,
            action_sender,
            timestamp,
            memo

        """

        recepies_string = str(recipies).replace("\'", "\"")
        url = "https://interop-mainnet.hasura.app/v1/graphql"

        headers = {
            'Content-Type': 'application/json',
        }

        payload = json.dumps({
            "query": "query MyQuery {nft_activities(where: {receipt_id: {_in: " + recepies_string + \
            "}}){receipt_id,price,kind,token_id,nft_contract_id,tx_sender,timestamp,memo}}"
        })

        response = requests.request("POST", url, headers=headers, data=payload)

        return pd.DataFrame(response.json()["data"]["nft_activities"])
