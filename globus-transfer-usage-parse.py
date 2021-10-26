#!/soft/xsede-globusauth-usage/python/bin/python3
###############################################################################
# Parse a Globus transfer usage file and return standard usage in CSV
# Usage:  ./<script> [<input_file>]
# Input:  text <input_file> or stdin, accept gzip'ed (.gz) <input_file>
# Output: stdout in CSV
###############################################################################
from datetime import datetime
import csv
import gzip
import re
import sys

from backports.datetime_fromisoformat import MonkeyPatch
MonkeyPatch.patch_fromisoformat()

# All possible output fields
#   Required:     'USED_COMPONENT', 'USE_TIMESTAMP', 'USE_CLIENT',
#   Recommended:  'USE_USER', 'USED_COMPONENT_VERSION', 'USED_RESOURCE'
#   Optional:     'USAGE_STATUS', 'USE_AMOUNT', 'USE_AMOUNT_UNITS'

#==== CUSTOMIZATION VARIABLES ====================

OUTPUT_DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'       # A datetime.strftime format
# The fields we are generating
OUTPUT_FIELDS = ['USED_COMPONENT', 'USE_TIMESTAMP', 'USE_CLIENT', 'USE_USER', 'USED_RESOURCE', 'USAGE_STATUS', 'USE_AMOUNT', 'USE_AMOUNT_UNITS']

# Regex for username from email
REGEX_USERATDOMAIN = r'^([^@]+)@([^@]+)$'

OLD_HEADERS = ('user_name','task_type','request_time','completion_time','source_endpoint_owner','source_endpoint','destination_endpoint_owner','destination_endpoint','source_shared_endpoint_host_owner','source_shared_endpoint_host','destination_shared_endpoint_host_owner','destination_shared_endpoint_host','status','bytes_transferred','files_processed','directories_processed','successful','failed','expired','canceled','skipped','bytes_checksummed','faults','checksum_faults','directory_expansions','taskid','duration(secs)','sync_level','encrypt_data','verify_checksum','preserve_modification_time','sync_delete')

# Add filter code below if desired

#==== END OF CUSTOMIZATIONS ====================

if __name__ == '__main__':
    if len(sys.argv) > 1:
        file = sys.argv[1]      # First arg after program name, 0 indexed
        if file[-3:] == '.gz':
            input_fd = gzip.open(file, mode='rt')
        else:
            input_fd = open(file, 'rt')
    else:
        input_fd = sys.stdin

    file_nopath = file[file.rfind('/')+1:]       # Works even if slash returned -1
    hyphen = file_nopath.find('-')
    if hyphen == -1:
        file_nodate = file_nopath
    else:
        file_nodate = file_nopath[:hyphen]
    filewords = file_nodate.split('.')
    if filewords[-1] == 'gz':
        filewords.pop()
    if filewords[-1] in ('csv', 'usage', 'log'):
        filewords.pop()
    
    input = csv.reader(input_fd, delimiter=',', quotechar='"')
    output = csv.writer(sys.stdout, delimiter=',', quotechar='|')
    headers = None
    
    TEMPLATE = {'USED_COMPONENT': 'org.globus.transfer',
                'USED_COMPONENT_VERSION': None,
                'USE_CLIENT': 'globus.org',
                'USE_AMOUNT_UNITS': 'bytes'}

    matches = 0
    for raw in input:
        if not headers:
            if raw[0] == 'user_name':         # It's the headers
                headers = list(raw)
                continue                                # Next input row
            headers = OLD_HEADERS
        line = dict(zip(headers,raw))
            
        o = TEMPLATE.copy()         # Initialize

        o['USE_TIMESTAMP']          = datetime.fromisoformat( line['request_time'] ).strftime( OUTPUT_DATE_FORMAT )

        matchObj = re.search(REGEX_USERATDOMAIN, line['user_name'])
        if matchObj and matchObj.group(2) == 'xsede.org':
            o['USE_USER'] = 'xsede:' + matchObj.group(1)
        else:
            o['USE_USER'] = 'local:' + line['user_name']
                
        o['USED_RESOURCE']          = 'transfer {}/files'.format(line['files_processed'])

        o['USAGE_STATUS']           = line['status']

        o['USE_AMOUNT']             = line['bytes_transferred']

        # PLACE FILTER CODE HERE
        # if <filter_expression>:
        #   continue

        matches += 1
        if matches == 1:
            output.writerow(OUTPUT_FIELDS)
        output.writerow([o[f] for f in OUTPUT_FIELDS])
