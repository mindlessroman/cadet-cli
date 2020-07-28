#!/usr/bin/env python
"""
This CLI tool allows a user to upload CSV, TSV, & JSON files to an Azure CosmosDB instance.
"""
import csv
import os
import sys
import click
import json
from azure.cosmos import CosmosClient

DELIMITERS = {
    'CSV': ',',
    'TSV': '\t'
}

FILE_ENDINGS = ('.CSV', '.TSV', '.JSON')


@click.version_option('1.1.0')
@click.group()
def cadet():
    """
    Acts as a super-group for the import command and any future commands to cadet
    """
    pass


@cadet.command(name='import')
@click.option(
    '--connection-string', '-s',
    help='The connection string for the database'
    )
@click.option(
    '--uri', '-u',
    help='The endpoint URI for the CosmosDB instance'
    )
@click.option(
    '--primary-key', '-k', '--key',
    help='The key provided to access the CosmosDB database'
)
@click.option(
    '--database-name', '-d', '--database',
    help='The name of the database to connect to',
    required=True
    )
@click.option(
    '--collection-name', '-c', '--collection', '--container', '--container-name',
    help='The collection/container to load the data into',
    required=True
    )
@click.option(
    '--file-type', '-t', '--type',
    help='The source file\'s type (Options: csv, tsv, json)',
    required=True
)
@click.argument('source')
def upload(source, file_type, collection_name, database_name, primary_key, uri, connection_string):
    """
    Given a source file `source` of type `file_type`:
        1. connects to the Cosmos DB instance using either
            (a) `primary_key` and `uri`
            OR
            (b) `connection_string`
        2. ...and uploads the data to the collection `collection_name` on database `database_name

    Assumes that the Cosmos DB subscription has both the database and the collection already
    made when running this tool.
    """
    file_type = file_type.upper()
    # Make sure it's a CSV, TSV, or JSON
    if not source.upper().endswith(FILE_ENDINGS):
        raise click.BadParameter('We currently only support CSV, TSV, and JSON uploads from Cadet')

    # You must have either the connection string OR (endpoint and key) to connect
    if (uri is None or primary_key is None) and (connection_string is None):
        raise click.BadParameter(
            'REQUIRED: Connection string OR *both* a URI and a key'
            )
    elif uri is not None and primary_key is not None:
        # Connect to Cosmos, then to the database, then the container/collection
        try:
            cosmos_client = CosmosClient(uri, {'masterKey': primary_key})
        except:
            raise click.BadParameter('Authentication failure to Azure Cosmos')
    elif connection_string is not None:
        try:
            cosmos_client = CosmosClient.from_connection_string(connection_string)
        except:
            # ...Unless they don't provide a usable connection string
            raise click.BadParameter('The connection string is not properly formatted.')

    # Read and upload at same time
    try:
        source_path = get_full_source_path(source)
        container = get_upload_client(cosmos_client, database_name, collection_name)
        read_and_upload(source_path, file_type, container)
    except FileNotFoundError as err:
        raise click.FileError(source, hint=err)

def get_full_source_path(source):
    return os.path.join(os.path.dirname(__file__), source)

def get_upload_client(cosmos_client, database_name, container_name):
    """
    Connects to the Cosmos instance via passed in cosmos_client,
    and returns the container's connection proxy for that
    database-container
    combination
    """

    database = cosmos_client.get_database_client(database_name)
    return database.get_container_client(container_name)


def read_and_upload(source_path, file_type, container):
    """
    Reads the `source` of type `file_type`, connects combination found in `container`
    """

    with open(source_path, 'r') as source_file:
        click.echo('Starting the upload')

        # Stats read for percentage done
        source_size = os.stat(source_path).st_size
        click.echo('Source file total size is: %s bytes' % source_size)

        # if CSV or TSV
        if file_type in DELIMITERS.keys():
            read_and_upload_csv(source_file, file_type, container, source_size)
        else: # if JSON
            read_and_upload_json(source_file, container, source_size)


def read_and_upload_csv(source_file, file_type, container, source_size):
    """
    Handles the reading of the CSV/TSV files, then uploads them via the container link.
    source_size is handed between the methods in order to communicate information to the
    user and not for functionality.
    """
    csv_reader = csv.reader(source_file, delimiter=DELIMITERS[file_type])
    line_count = 0
    document = {}

    with click.progressbar(length=source_size, show_percent=True) as status_bar:
        for row in csv_reader:
            if line_count == 0:
                csv_cols = row
                line_count += 1
            else:
                for ind, col in enumerate(csv_cols):
                    document[col] = row[ind]

                try:
                    container.upsert_item(document)
                    status_bar.update(sys.getsizeof(row))
                except:
                    raise click.ClickException('Upload failed')
        click.echo('Upload complete!')

def read_and_upload_json(source_file, container, source_size):
    """
    Handles the reading of a JSON file, then uploads them via the container link.
    source_size is handed between the methods in order to communicate information to the
    user and not for functionality.
    """
    json_array = json.load(source_file)
    with click.progressbar(length=len(json_array) , show_percent=True) as status_bar:
        for item in json_array:
            try:
                container.upsert_item(item)
                status_bar.update(sys.getsizeof(item))
            except:
                raise click.ClickException('Upload failed:', sys.exc_info())
        click.echo('Upload complete!')


if __name__ == '__main__':
    cadet()
