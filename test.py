import unittest
import data_connectors as dc
import config as c
import json
import os
import google
import google_cloud_storage_utils as csu
import datetime

# TESTING DATA CONNECTORS

class TestMetronomoTXCloudStorageConnector(unittest.TestCase):

    def setUp(self):
        if (os.path.isfile(c.MetronomoTXCloudStorageConnector_LOCAL_TOKEN_JSON_PATH)):
            self.token_json_path = c.MetronomoTXCloudStorageConnector_LOCAL_TOKEN_JSON_PATH
        elif (os.path.isfile(c.MetronomoTXCloudStorageConnector_TOKEN_JSON_PATH)):
            self.token_json_path = c.MetronomoTXCloudStorageConnector_TOKEN_JSON_PATH
        else:
            raise ValueError("Valid token json path is not provided")

    def test_not_existing_key_file(self):
        with self.assertRaises(FileNotFoundError) as exception_context:
            dc.MetronomoTXCloudStorageConnector(
                "some-not-existing-bucket",
                "some_not_existing_key.json",
                "wrong network",
                [],
                "wrong_granularity"
            ).getData()

        self.assertEqual(
            str(exception_context.exception),
            "[Errno 2] No such file or directory: 'some_not_existing_key.json'"
        )


    def test_key_file_with_no_key(self):
        with self.assertRaises(json.decoder.JSONDecodeError) as exception_context:
            dc.MetronomoTXCloudStorageConnector(
                "some-not-existing-bucket",
                c.MetronomoTXCloudStorageConnector_LOCAL_TEST_JSON_WITHOUT_KEY,
                "wrong network",
                [],
                "wrong_granularity"
            ).getData()

        self.assertEqual(
            str(exception_context.exception),
            "Expecting value: line 1 column 1 (char 0)"
        )

    def test_key_file_with_no_permissions(self):
        with self.assertRaises(google.api_core.exceptions.Forbidden) as exception_context:
            dc.MetronomoTXCloudStorageConnector(
                "test_some_test",
                c.MetronomoTXCloudStorageConnector_LOCAL_TEST_JSON_WITH_NO_PERMISSIONS,
                "wrong network",
                [],
                "wrong_granularity"
            ).getData()

        self.assertEqual(
            str(exception_context.exception),
            "403 GET https://storage.googleapis.com/storage/v1/b/test_some_test?projection=noAcl&prettyPrint=false: test-no-permission@web3advertisement.iam.gserviceaccount.com does not have storage.buckets.get access to the Google Cloud Storage bucket. Permission 'storage.buckets.get' denied on resource (or it may not exist)."
        )


    def test_wrong_bucket(self):
        with self.assertRaises(google.api_core.exceptions.NotFound) as exception_context:
            dc.MetronomoTXCloudStorageConnector(
                "some-not-existing-bucket",
                self.token_json_path,
                "wrong network",
                [],
                "wrong_granularity"
            ).getData()

        self.assertEqual(
            str(exception_context.exception),
            "404 GET https://storage.googleapis.com/storage/v1/b/some-not-existing-bucket?projection=noAcl&prettyPrint=false: The specified bucket does not exist."
        )

    def test_wrong_network_not_in_blob_paths(self):
        with self.assertRaises(ValueError) as exception_context:
            dc.MetronomoTXCloudStorageConnector(
                "test_some_test",
                self.token_json_path,
                "not_existing_netowrk_path",
                [],
                "wrong_granularity"
            ).getData()
        self.assertEqual(
            str(exception_context.exception),
            "Wrong network provideded : not_existing_netowrk_path. Please choose correct network : mainnet, testnet"
        )

    def test_wrong_network_not_in_blobs(self):
        with self.assertRaises(ValueError) as exception_context:
            dc.MetronomoTXCloudStorageConnector(
                "test_some_test",
                self.token_json_path,
                "testnet",
                [],
                "wrong_granularity"
            ).getData()
        self.assertEqual(
            str(exception_context.exception),
            "Wrong network provideded : testnet. Network not in bucket"
        )

class TestMintbaseNFTActivitiesConnector(unittest.TestCase):
    def setUp(self):
        self.nonNFTRecipies = [
            "2N1cQkZNG3cdakT7z2G2FwoV5Kp8H9iASZDKMNbSGfUZ",
            "8TECfjQDWuxTacydyRrVT9QXknsRypFDRzjwnFXK3HiY",
            "D9eCqj4vPVaodYZPpUYNy2JGmujMXPR1ASkiuK6tsTsQ"
       ]
        self.NFTRecipies = [
            "kaubUw65ehPv9hmS4BNWbKhg5on1AjvRypxDLw3dyZh",
            "",
            "",
        ]


class TestGoogleCloudStorageUtils(unittest.TestCase):

# TODO:
# there is a problem with not closed socket during performing this test.
# Have not resolved it yet

    def setUp(self):
        if (os.path.isfile(c.MetronomoTXCloudStorageConnector_LOCAL_TOKEN_JSON_PATH)):
            self.token_json_path = c.MetronomoTXCloudStorageConnector_LOCAL_TOKEN_JSON_PATH
        elif (os.path.isfile(c.MetronomoTXCloudStorageConnector_TOKEN_JSON_PATH)):
            self.token_json_path = c.MetronomoTXCloudStorageConnector_TOKEN_JSON_PATH
        else:
            raise ValueError("Valid token json path is not provided")

        self.correct_tx_blobs_list = list(["mainnet/daily_data/transactions/2022-01-04", "mainnet/daily_data/transactions/2022-01-05"])
        self.correct_actions_blobs_list = list(["mainnet/daily_data/action_receipt_actions/2022-01-05", "mainnet/daily_data/action_receipt_actions/2022-01-04"])
        self.wrong_blobs_list_index_error = list(["2022-01-04"])
        self.wrong_blobs_list_wrong_date_format = list(["mainnet/daily_data/action_receipt_actions/20220104", "mainnet/daily_data/action_receipt_actions/04012022"])
        self.wrong_blobs_list_wrong_path = list(["mainnet/daily_data/transactions//2022-01-04"])


    def test_get_bucket_not_existing_key_file(self):
            with self.assertRaises(FileNotFoundError) as exception_context:
                csu.get_bucket("not_existing_token.json", "test_some_test")
            self.assertEqual(
                str(exception_context.exception),
                "[Errno 2] No such file or directory: 'not_existing_token.json'"
        )

    def test_get_bucket_wrong_format_key_file(self):
        with self.assertRaises(json.decoder.JSONDecodeError) as exception_context:
            csu.get_bucket(c.MetronomoTXCloudStorageConnector_LOCAL_TEST_JSON_WITHOUT_KEY, "test_some_test")
        self.assertEqual(
            str(exception_context.exception),
            "Expecting value: line 1 column 1 (char 0)"
        )

    def test_get_bucket_no_permissions_key_file(self):
        with self.assertRaises(google.api_core.exceptions.Forbidden) as exception_context:
            csu.get_bucket(c.MetronomoTXCloudStorageConnector_LOCAL_TEST_JSON_WITH_NO_PERMISSIONS, "test_some_test")
        self.assertEqual(
            str(exception_context.exception),
            "403 GET https://storage.googleapis.com/storage/v1/b/test_some_test?projection=noAcl&prettyPrint=false: test-no-permission@web3advertisement.iam.gserviceaccount.com does not have storage.buckets.get access to the Google Cloud Storage bucket. Permission 'storage.buckets.get' denied on resource (or it may not exist)."
        )

    def test_get_bucket_not_existing_bucket(self):
        with self.assertRaises(google.api_core.exceptions.NotFound) as exception_context:
            csu.get_bucket(self.token_json_path, "not_existing_bucket_test")
        self.assertEqual(
            str(exception_context.exception),
            "404 GET https://storage.googleapis.com/storage/v1/b/not_existing_bucket_test?projection=noAcl&prettyPrint=false: The specified bucket does not exist."
        )

    def test_get_blob_list(self):
        bucket = csu.get_bucket(self.token_json_path, "test_some_test")
        self.assertEqual(len(csu.get_blob_list(bucket.client, bucket)), 1)
        self.assertEqual(csu.get_blob_list(bucket.client, bucket)[0], "test.json")

    def test_filter_blobs_by_dates(self):
        self.assertEqual(
            csu.filter_blobs_by_dates(
                self.correct_tx_blobs_list,
                [datetime.datetime.strptime("2022-01-04", "%Y-%m-%d").date()]
            ),
            list(["mainnet/daily_data/transactions/2022-01-04"])
        )
        self.assertEqual(
            csu.filter_blobs_by_dates(
                self.correct_actions_blobs_list,
                [datetime.datetime.strptime("2022-01-04", "%Y-%m-%d").date()]
            ),
            list(["mainnet/daily_data/action_receipt_actions/2022-01-04"])
        )
        self.assertEqual(
            csu.filter_blobs_by_dates(
                self.wrong_blobs_list_wrong_path,
                [datetime.datetime.strptime("2022-01-04", "%Y-%m-%d").date()]
            ),
            []
        )

        self.assertEqual(
            csu.filter_blobs_by_dates(
                self.wrong_blobs_list_wrong_date_format,
                [datetime.datetime.strptime("2022-01-04", "%Y-%m-%d").date()]
            ),
            []
        )
        with self.assertRaises(ValueError) as exception_context:
            csu.filter_blobs_by_dates(
                self.wrong_blobs_list_index_error,
                [datetime.datetime.strptime("2022-01-04", "%Y-%m-%d").date()]
            )
        self.assertEqual(
            str(exception_context.exception),
            "Not valid TX or Actions paths. Please provide correct paths list"
        )

    def test_filter_blobs_by_path(self):
        self.assertEqual(
            csu.filter_blobs_by_path(
                self.correct_tx_blobs_list,
                "mainnet/daily_data/transactions/"
            ),
            self.correct_tx_blobs_list
        )


        self.assertEqual(
            csu.filter_blobs_by_path(
                self.correct_actions_blobs_list,
                "mainnet/daily_data/transactions/"
            ),
            []
        )

        with self.assertRaises(TypeError) as exception_context:
            csu.filter_blobs_by_path(
                list([1,2,3]),
                "mainnet/daily_data/transactions/"
            )
        self.assertEqual(
            str(exception_context.exception),
            "argument of type 'int' is not iterable"
        )

