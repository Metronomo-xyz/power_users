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
1. Clone this repository
2. Implement DataConnector child class with getData method to get receipt_id of interested transactions
3. Change __main__.py to get data from your data connector (created on step 2) and provide MintbaseNFTActivityDataConnector with list of receipt_ids

## Self-hosted usage as a module
1. Clone this repository
2. Use power_users.py in your project as a module to find power users with provided data

## User this module as a service
Not available yet. Please, text https://t.me/yaroslav_bondarchuk if needed 
