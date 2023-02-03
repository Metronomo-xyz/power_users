# Power users module

Power users module allows finding most valuable users for given Mintbase NFT smart contract

This users might be used as reference for creating look-alike and other targeting mechanics

Supposed use case:

1. Find list of power users of a target Mintbase NFT smart contract
2. Pass this list to module that will find similar users in the whole network (will be developed as future work)
3. Get the list of potential power users across the whole network
4. Get the list of project with the highest number of potential power users
5. Communicate with potential power users directly via airdrops, communication protocols or any other available method (right now communication options are quite limited, but we hope that in nearest future this topic will have number of tools)
6. Communicate with potential power users via co-promotions company with projects from step 4


## Self-hosted usage as a standalone script


###0. Clone this repository
```
git clone https://github.com/Metronomo-xyz/power_users.git
```

###1. Chose the way how to use power_users module

It's possible to use power_users method with public available data in Google Cloud Storage Bucket `near-data-public`
There is only data needed for power-user module in `near-data-public` bucket. Inspect the bucket or the `config.py` to get the bucket structure.
To use module with public data user `-p` option

Other way is to implement your own data connector (inherit `DataConnector` class). Data connector to transactions data must have method `getData`, which return pandas `DataFrame` with `["signer_account_id", "receiver_account_id", "converted_into_receipt_id"]` columns
Implement DataConnector child class with getData method to get receipt_id of interested transactions

Next steps will describe if you want to use public data

###2. Create virtual environment

It's recomended to use virtual environment while using module

If you don't have `venv` installed run (ex. for Ubuntu)
```
sudo apt-get install python3-venv

```
then create and activate virtuale environmnt
```
python3 -m venv power_users
source power_users/bin/activate
```

### 3. Install requirements
Run
```
pip install -r power_users/requirements.txt
```
### 4. Create google auth default credentials 
The easiest method is to run

```gcloud auth application-default login --no-launch-browser```

and then copy authentication link to browes and then copu code from browser to shell.

For other ways to create credentials see

[https://cloud.google.com/docs/authentication/provide-credentials-adc](https://cloud.google.com/docs/authentication/provide-credentials-adc)

### 5. Run the module

```python3 -m power_users -c voiceoftheoceans.mintbase1.near -p -s 12122022 -r 30```

### 6. Possible options:

- `-p`, `--public-data` to use module with public data
- `-n`, `--netowrk` to choose NEAR network. Currently, only `mainnet` is available
- `-b`, `--bucket` to chose the bucket from which to take the data
- `-t, `--token-json-path` to provide token json file path
- `-s`, `--start_date` the last date of the dates period in `ddmmyyyy` format
- `-r`, `--date_range` number of days to take into power users calculation. For example, if start date is 12122022 and range 30 then dates will be since 13-11-2022 to 12-12-2022 inclusively
- `-c`, `--target_smart_contract` smart contract ot analyze. It should be NFT contract

### 7. Get the result
For now module returns list of wallets, which have the most value for smart contract according to their interaction activity.
It;s possible to just copy result from shell, but better way is to change `__main__.py` to store results as `.csv` or any other suitable for you file type


## Self-hosted usage as a module
1. Clone this repository
2. Use power_users.py in your project as a module to find power users with provided data

## User this module as a service
Not available yet. Please, text https://t.me/yaroslav_bondarchuk if needed 
