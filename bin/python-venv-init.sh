#!/bin/bash

read -p "Python venv directory [/soft/xsede-globusauth-usage/python]: " VENV_BASE
VENV_BASE=${VENV_BASE:-/soft/xsede-globusauth-usage/python}
echo $VENV_BASE

#export LD_LIBRARY_PATH=/soft/python-current/lib

CMD="virtualenv --python=python3 ${VENV_BASE}"
echo Executing: ${CMD}
${CMD}

CMD="source ${VENV_BASE}/bin/activate"
echo Executing: ${CMD}
${CMD}

CMD="pip3 install --upgrade pip"
echo Executing: ${CMD}
${CMD}

CMD="pip3 install pytz"
echo Executing: ${CMD}
${CMD}

CMD="pip3 install globus-sdk"
echo Executing: ${CMD}
${CMD}

CMD="pip3 install utils"
echo Executing: ${CMD}
${CMD}
