# xsede-usage-globus

Processes Globus Auth usage data which is provided by Globus and transforms it into an XCI compatible format as described in [XCI Software Usage Metrics Collection Service - Design Document](https://docs.google.com/document/d/1l9Ww8OR5QaWC9tqM1mnSfelhzNdZ1Nz972bRw1aLZ1I/edit?usp=sharing).

These programs have only been tested on our XCI metrics server and provide the following functions:

**sync-globus-transfer-dirs.py** - Copies Globus Auth usage files from Globus to the XCI metrics server.

**globusauth-map-uuid-to-hostname.py** - Translates Globus Auth client ids to hostnames to comply with above metrics design.  The mapping is pulled from a Google doc that is manually maintained.  This runs nightly under the metrics account using the following command: 
```
/soft/xsede-globusauth-usage/python/bin/python3 /soft/xsede-globusauth-usage/PROD/bin/globusauth-map-uuid-to-hostname.py --output /soft/xsede-globusauth-usage/var/globusauth-endpoints.json
```

**globusauth-usage-parse.py** - Converts Globus Auth usage file to compliant format.  This is instantiated under the metrics account on the XCI metrics server nighly using the following command: 
```
/soft/XCI-Usage-Tools/PROD/usage-process/bin/repository_process.py -c /soft/xsede-globusauth-usage/conf/repository_process_globusauth.conf
```
