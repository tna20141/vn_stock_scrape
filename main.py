import getopt, sys
from datetime import datetime, timedelta

import importer
import ticker

options = 's:e:o:'

def _execute(fromTime, toTime):
    print('Importing from ' + str(fromTime) + ' to ' + str(toTime))
    importer.doImport(fromTime, toTime)
    ticker.cleanup()

try:
    fromTime = None
    toTime = None
    arguments, values = getopt.getopt(sys.argv[1:], options)
    for currentArgument, currentValue in arguments:
        if currentArgument == '-o':
            fromTime = datetime.today() - timedelta(days=int(currentValue))
            _execute(fromTime, toTime)
            exit(0)
        elif currentArgument == '-s':
            fromTime = datetime.strptime(currentValue, '%Y-%m-%d')
        elif currentArgument == '-e':
            toTime = datetime.strptime(currentValue, '%Y-%m-%d')
    _execute(fromTime, toTime)
except Exception as e:
    ticker.cleanup()
    print(str(e))
