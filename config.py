"""
Config file with constants and other configutrations for your data connectors
All the constants related to specific connectors have name of the connector in the constant name
"""
""""""
MetronomoTXCloudStorageConnector_DEFAULT_PROJECT = "web3advertisement"
MetronomoTXCloudStorageConnector_DEFAULT_BUCKET_NAME = "near-data-public"
MetronomoTXCloudStorageConnector_DEFAULT_NETWORK = "mainnet"
MetronomoTXCloudStorageConnector_DEFAULT_GRANULARITY = "daily"

MetronomoTXCloudStorageConnector_ENTITIES = {
    "transactions" : {
        "fields": ["signer_account_id", "receiver_account_id", "converted_into_receipt_id"]
    },
    "actions" : {
        "fields": ["receipt_id", "action_kind"]
    }
}

MetronomoTXCloudStorageConnector_BLOB_PATHS = {
    "mainnet": {
        "hourly": {
            "transactions" : "mainnet/hourly_data/transactions/",
            "actions" : "mainnet/hourly_data/action_receipt_actions/"
        },
        "daily": {
            "transactions": "mainnet/daily_data/transactions/",
            "actions": "mainnet/daily_data/action_receipt_actions/"
        }
    }
}