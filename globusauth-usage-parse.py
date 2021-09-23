#!/soft/xsede-globusauth-usage/python/bin/python3
################################################################################################
# Parse a Globus Auth usage data file and return standard XSEDE usage data format in csv
# Usage: ./<script> [<input_file>]
# Input: Globus Auth Usage data file in CSV format
# output: XSEDE Usage data file in CSV format
#################################################################################################
from datetime import datetime
import csv
import gzip
import pytz
import re
import sys

#Output fields:
# Required: 'USED_COMPONENT', 'USE_TIMESTAMP', 'USE_CLIENT', 'USE_USER'

#==== CUSTOMIZATION VARIABLES ==================

INPUT_TZ = 'UTC'                     # One of pytz.common_timezones
INPUT_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'  # A datetime.strptime format
# The fields we are generating
OUTPUT_FIELDS = ['USED_COMPONENT', 'USE_TIMESTAMP', 'USE_CLIENT', 'USE_USER']

# Regex for Program
# Regex for username from email
REGEX_USERNAME = r'^([^@]+)@[^@]+$'
# Regex for domain from email
REGEX_DOMAIN = r'(?<=@)(\S+$)'

#==== END OF CUSTOMIZATIONS ====================

if __name__ == '__main__':
    if len(sys.argv) > 1:
        file = sys.argv[1]
    else:
        print('Please provide a CSV filename.')
        sys.exit(0)

    output = csv.writer(sys.stdout, delimiter=',',quotechar='|')

    matches = 0 
    with gzip.open(file, 'rt') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        next(readCSV)
        for row in readCSV:
                
            o = {}
            o['USED_COMPONENT'] = 'org.globus.auth'
            
            row[0] = row[0][:-6]
            date_string = row[0].strip()
            # some files don't contain microsecond
            if '.' not in date_string:
                date_string = date_string + '.0'
            dtm = datetime.strptime(date_string, INPUT_DATE_FORMAT)
            o['USE_TIMESTAMP'] = pytz.timezone(INPUT_TZ).localize(dtm).astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            o['USE_CLIENT'] = row[1]

            matchObj = re.search(REGEX_USERNAME, row[2])
            matchObj2 = re.search(REGEX_DOMAIN, row[2])
            if matchObj2.group(1) == 'xsede.org':
                if matchObj:
                    o['USE_USER'] =  'xsede:'  + matchObj.group(1)
                else:
                    o['USE_USER'] = ''
            else:
                o['USE_USER'] = 'local:' + row[2]

            matches += 1
            if matches == 1:
                output.writerow(OUTPUT_FIELDS)
            output.writerow([o[f] for f in OUTPUT_FIELDS])
            
