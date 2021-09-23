#!/soft/xsede-globusauth-usage/python/bin/python3

import argparse
import csv
import json
import urllib
import urllib.request as req
import urllib.parse as pa
from urllib.error import URLError, HTTPError
import io

url='https://docs.google.com/spreadsheets/d/1_Rip3HpCinrrxd0cH9-S1MkUi1i-Q03aFdNdOYhOXOw/export?format=csv'
req1 = req.Request(url)

parser = argparse.ArgumentParser(description='Get Globus auth endpoint ids from Google doc')
parser.add_argument('--output', default='endpoints.json', help='Specify output file for mappings')
args = parser.parse_args()


try:
	response = req.urlopen(req1)
except HTTPError as e:
	print("Error Code: ", e.code)

except URLError as e:
	print("Reason: ", e.reason)
else:
	resp_data = response.read()
	encoding = response.info().get_content_charset('utf-8')
	usage_reports = resp_data.decode(encoding)

data_list = {}

sio = io.StringIO (usage_reports, newline=None)
reader = csv.DictReader(sio, dialect=csv.excel)
rownum = 0

for row in reader:
	client_id = row.get("Globus Client ID",row.get("Client ID"))
	hostname = row["Hostname"]
	data_list[client_id] = "MAP:"+hostname

with open(args.output, 'w') as outfile:
	json.dump(data_list, outfile, indent=4)	
