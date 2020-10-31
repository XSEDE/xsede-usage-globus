#!/usr/bin/env python

import argparse
import json
import os
import time
import sys
import webbrowser

from utils import enable_requests_logging, is_remote_session

from globus_sdk import (NativeAppAuthClient, TransferClient,
                        RefreshTokenAuthorizer)
from globus_sdk.exc import GlobusAPIError
from globus_sdk import TransferData

REDIRECT_URI = 'https://auth.globus.org/v2/web/auth-code'
SCOPES = ('openid email profile '
          'urn:globus:auth:scope:transfer.api.globus.org:all')

parser = argparse.ArgumentParser(description='Sync usage files via Globus Transfer')
parser.add_argument('--token', default='refresh-tokens.json', help='Specify token file to be able to automate/refresh')
parser.add_argument('--config', required=True, help='Specify transfer args in config file')
args = parser.parse_args()

config_path = os.path.abspath(args.config)
config = {}
try:
    with open(config_path, 'r') as cf:
        config = json.load(cf)
except Exception as e:
    sys.stderr.write('ERROR "{}" parsing config={}\n'.format(e, config_path))
    sys.exit(1)

TOKEN_FILE = args.token
CLIENT_ID = config["client_id"]
SRC_ENDPOINT_ID = config["source_endpoint_id"]
DEST_ENDPOINT_ID = config["dest_endpoint_id"]
SRC_DIR = config["source_dir"]
DEST_DIR = config["dest_dir"]

get_input = getattr(__builtins__, 'raw_input', input)

# uncomment the next line to enable debug logging for network requests
# enable_requests_logging()


def load_tokens_from_file(filepath):
    """Load a set of saved tokens."""
    with open(filepath, 'r') as f:
        tokens = json.load(f)

    return tokens


def save_tokens_to_file(filepath, tokens):
    """Save a set of tokens for later use."""
    with open(filepath, 'w') as f:
        json.dump(tokens, f)


def update_tokens_file_on_refresh(token_response):
    """
    Callback function passed into the RefreshTokenAuthorizer.
    Will be invoked any time a new access token is fetched.
    """
    save_tokens_to_file(TOKEN_FILE, token_response.by_resource_server)


def do_native_app_authentication(client_id, redirect_uri,
                                 requested_scopes=None):
    """
    Does a Native App authentication flow and returns a
    dict of tokens keyed by service name.
    """
    client = NativeAppAuthClient(client_id=client_id)
    # pass refresh_tokens=True to request refresh tokens
    client.oauth2_start_flow(requested_scopes=requested_scopes,
                             redirect_uri=redirect_uri,
                             refresh_tokens=True)

    url = client.oauth2_get_authorize_url()

    print('Native App Authorization URL: \n{}'.format(url))

    if not is_remote_session():
        webbrowser.open(url, new=1)

    auth_code = get_input('Enter the auth code: ').strip()

    token_response = client.oauth2_exchange_code_for_tokens(auth_code)

    # return a set of tokens, organized by resource server name
    return token_response.by_resource_server


def main():
    tokens = None
    try:
        # if we already have tokens, load and use them
        tokens = load_tokens_from_file(TOKEN_FILE)
    except:
        pass

    if not tokens:
        # if we need to get tokens, start the Native App authentication process
        tokens = do_native_app_authentication(CLIENT_ID, REDIRECT_URI, SCOPES)

        try:
            save_tokens_to_file(TOKEN_FILE, tokens)
        except:
            pass

    transfer_tokens = tokens['transfer.api.globus.org']

    auth_client = NativeAppAuthClient(client_id=CLIENT_ID)

    authorizer = RefreshTokenAuthorizer(
        transfer_tokens['refresh_token'],
        auth_client,
        access_token=transfer_tokens['access_token'],
        expires_at=transfer_tokens['expires_at_seconds'],
        on_refresh=update_tokens_file_on_refresh)

    transfer = TransferClient(authorizer=authorizer)

    # print out a directory listing from an endpoint
    try:
        transfer.endpoint_autoactivate(SRC_ENDPOINT_ID)
        transfer.endpoint_autoactivate(DEST_ENDPOINT_ID)
    except GlobusAPIError as ex:
        print(ex)
        if ex.http_status == 401:
            sys.exit('Refresh token has expired. '
                     'Please delete refresh-tokens.json and try again.')
        else:
            raise ex

    for entry in transfer.operation_ls(SRC_ENDPOINT_ID, path=SRC_DIR):
        print(entry['name'] + ('/' if entry['type'] == 'dir' else ''))

    tdata = TransferData(transfer, SRC_ENDPOINT_ID, DEST_ENDPOINT_ID, label="XCI Metrics", sync_level=0, verify_checksum=True, encrypt_data=True)
    tdata.add_item(SRC_DIR, DEST_DIR, recursive=True)
    transfer_result = transfer.submit_transfer(tdata)
    print("task_id =", transfer_result["task_id"])

    # status = 'ACTIVE', 'SUCCEEDED', 'FAILED', 'INACTIVE'
    status = transfer.get_task(transfer_result["task_id"])
    print('STATUS =', status['status'])
    while status['status'] == 'ACTIVE':
        time.sleep(5)
        status = transfer.get_task(transfer_result["task_id"])
        print('STATUS =',status['status'])

    print('FINAL RESULT =', status['status'])
    status_message = "{} new files transferred, {} total files, {} files unchanged".format(status['files_transferred'], status['files'], status['files_skipped'])
    print(status_message)

if __name__ == '__main__':
    main()
