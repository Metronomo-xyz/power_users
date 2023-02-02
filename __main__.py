from power_users import data_connectors as dc
from power_users import power_users as pu
import getopt
import sys
import datetime


if __name__ == '__main__':
    argv = sys.argv[1:]
    options = "lpn:b:s:r:c:"
    long_options = ["local", "public-data", "network=", "bucket=", "start_date=", "date_range=", "target_smart_contract="]

    entities = list()
    start_date = datetime.date.today() - datetime.timedelta(days=1)
    dates_range = 1
    target_smart_contract = None
    run_local = False
    with_public_data = False

    try:
        opts, args = getopt.getopt(argv, options, long_options)

        for opt, value in opts:
            if opt in ("-l", "--local"):
                run_local = True

            elif opt in ("-p", "--public-data"):
                with_public_data = True

            elif opt in ("-n", "--network"):
                network = value

            elif opt in ("-b", "--bucket"):
                bucket_name = value

            elif opt in ("-s", "--start_date"):
                try:
                    start_date = datetime.datetime.strptime(value, "%d%m%Y").date()
                except ValueError as e:
                    print("ERROR OCCURED: --start_date must be in %d%m%Y format, but " + value + " was given")
                    sys.exit(1)

            elif opt in ("-r", "--date_range"):
                try:
                    dates_range = int(value)
                except ValueError as e:
                    print("ERROR OCCURED: --date-range must be integer, but " + value + " was given")
                    sys.exit(1)

            elif opt in ("-c", "--target_smart_contract"):
                target_smart_contract = value

    except getopt.GetoptError as e:
        print('Error while parsing command line arguments : ' + str(e))

    if (target_smart_contract is None):
        raise ValueError("Target smart contract is not provided. Please, use -c or --target_smart_contract option to provide target smart contract value")

    print("args handled")

    dates = [start_date - datetime.timedelta(days=x) for x in range(dates_range)]

    print("getting TX data")
    gcs_connector = dc.MetronomoTXCloudStorageConnector(dates, run_local=run_local, with_public_data=with_public_data)
    print(gcs_connector)
    df = gcs_connector.getData()
    df["receiver_account_id"] = df["receiver_account_id"].apply(lambda x: str(x))

    recepies = df[df["receiver_account_id"].apply(
        lambda x: ("mintbase" in x) & (x != "mintbase.sputnik-dao.near")
        )]["converted_into_receipt_id"].tolist()

    print("recepies len : ")
    print(len(recepies))

    print("getting NFT Activity data : ")
    mintbaseNFTActivityConnector = dc.MintbaseNFTActivitiesConnector()
    print(mintbaseNFTActivityConnector)

    data = mintbaseNFTActivityConnector.getData(recepies)
    data = data[data["kind"] == "make_offer"]

    print("NFT Activity data len : " + str(len(data[data["kind"] == "make_offer"])))

    result = pu.getPowerUsers(data, target_smart_contract)
    print(result)
