"""
Imports module necessary for testing
"""
import copy
import os
from unittest import mock

# pylint: disable=W0622
# Reason: Redefined to use same error as SDK
from requests.exceptions import ConnectionError
from click.testing import CliRunner
from ..cadet import (
    upload
)

GOOD_CSV = 'test.csv'
GOOD_TSV = 'test.tsv'
GOOD_JSON = 'test.json'

CSV_TYPE = 'CSV'
TSV_TYPE = 'TSV'
JSON_TYPE = 'JSON'

TXT_TEST_FILE = 'a.txt'
CURR_DIRECTORY = os.path.dirname(__file__)

# Default Connection string, URI, Key, DB Name and Collection name are
# not actual instances of valid strings; currently used for testing purposes only
TEST_CONN_STRING = 'AccountEndpoint=https://testinguri.com:443/;AccountKey=testing==;'
TEST_URI = 'https://testinguri.documents.azure.com:443/'
TEST_KEY = 'testingkey=='

TEST_DB = 'TestDB'
TEST_COLLECTION = 'TestCollection'
RUNNER = CliRunner()

class MockCosmosClient:
    """
    Class constructed to mock Azure CosmosClient
    """

    def CosmosClient(self, uri, auth):
        raise ConnectionError("Authentication failure to Azure Cosmos")

    # pylint: disable=C0103
    # Reason: Using same method call as SDK
    def from_connection_string(self, conn_string):
        """
        Mocks Azure CosmosDB's CosmosClient
        """
        raise ConnectionError('Authentication failure to Azure Cosmos')

    # pylint: disable=C0103
    # Reason: Using same method call as SDK
    @classmethod
    def get_database_client(cls, database_name):
        """
        Mocks Azure CosmosClient's get_database_client
        """

        raise ConnectionError('Connection failure to CosmosDB instance\'s database')

class MockContainerClient:
    """
    The mock class for ContainerClientProxy
    """

    def __init__(self):
        self.upserted_docs = list()

    # pylint: disable=C0103
    # Reason: Using same method call as SDK
    def upsert_item(self, document):
        self.upserted_docs.append('your mom')
        self.upserted_docs.append(document)


class TestClass:
    """
    Class constructed to hold tests
    """

    # @mock.patch('src.cadet.get_full_source_path', autospec=True)
    # @mock.patch('src.cadet.get_upload_client', autospec=True)
    @mock.patch('src.cadet.CosmosClient', side_effect=MockCosmosClient().CosmosClient)
    # pylint: disable=R0201
    # Reason: R0201 makes pytest ignore test functions
    def test_good_params_uri_pkey_csv(self,
     mock_cosmosclient): #mock_get_full_source_path, mock_get_upload_client,
        """
        Tests that, given all required options, including a primary Key and URI combo
        and a CSV file, the tool works as expected
        """

        mock_cosmos_client = MockCosmosClient().CosmosClient
        mock_cosmosclient.return_value = mock_cosmos_client
        assert CosmosClient() == mock_cosmos_client

        mock_get_full_source_path.return_value = os.path.join(CURR_DIRECTORY, GOOD_CSV)
        mock_container_client = MockContainerClient()
        mock_get_upload_client.return_value = mock_container_client

        result = RUNNER.invoke(
            upload, [GOOD_CSV, '--type', CSV_TYPE, '-d', TEST_DB,
                     '-c', TEST_COLLECTION, '-u', TEST_URI, '-k', TEST_KEY]
            )
        print(result.output)
        print(mock_container_client.upserted_docs)

        expected_keys = ['county', 'eq_site_limit', 'policyID', 'statecode']
        expected_values = list()

        # Expected values from the test.csv file
        expected_values.append(['119736', '498960', 'CLAY COUNTY', 'FL'])
        expected_values.append(['1322376', '448094', 'CLAY COUNTY', 'FL'])
        expected_values.append(['190724', '206893', 'CLAY COUNTY', 'FL'])
        expected_values.append(['0', '333743', 'CLAY COUNTY', 'FL'])
        expected_values.append(['0', '172534', 'CLAY COUNTY', 'FL'])

        # Collection assert that expected values and actual upserted values are equal
        for num in range(len(mock_container_client.upserted_docs)):
            vals = mock_container_client.upserted_docs[num]

            keys = list(vals.keys())
            vals = list(vals.values())

            keys.sort()
            vals.sort()
            assert keys == expected_keys
            assert vals == expected_values[num]
        assert len(mock_container_client.upserted_docs) == 5
        assert result.exit_code == 0