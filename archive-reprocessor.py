# todo might make sense to just process the whole text file and insert a newline before every instance of
# {"Siri"


# archive-reprocessor.py
# 28 april 2021

from fnmatch import fnmatch
import os
import gzip
import shutil
import sys
from json_stream_parser import load_iter
from Parser import *
import csv
import datetime as dt
import ijson

def get_daily_filelist(path):
    daily_filelist=[]
    include_list = ['daily*.gz']
    for dirname, _, filenames in os.walk(path):
        for filename in filenames:
            if any(fnmatch(filename, pattern) for pattern in include_list):
                daily_filelist.append(filename)
    sorted_daily_filelist = sorted(daily_filelist, key=lambda daily_filelist: daily_filelist[6:16])
    return sorted_daily_filelist


if __name__ == "__main__":

    print ('started at {}'.format(dt.datetime.now().strftime('%H-%M')))
    datadir = os.getcwd()+'/data/'
    dailies = get_daily_filelist(datadir)

    for daily_filename in dailies:

        gzipfile = datadir + daily_filename
        ungzipfile = datadir + '{}.json'.format(daily_filename)

        # try to load the uncompressed file from disk
        try:
            f = open(ungzipfile, 'r')
            f.close()

        # if not exist, unzip it
        except:
            print('Unzipping {}{}'.format(datadir, daily_filename))
            with gzip.open(gzipfile, 'rb') as f_in:
                with open(ungzipfile, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)


        finally:
            # parse and dump all responses
            # https://pypi.org/project/json-stream-parser/
            sys.stdout.write('Parsing JSON responses and dumping to CSV.')

            # # json_stream_parser load_iter streaming decoder
            # with open(ungzipfile, 'r') as f:
            #     responseGenerator = (r for r in load_iter(f))

            # ijson stream decoder https://github.com/ICRAR/ijson instead?
            f = open(ungzipfile, 'r')
            responses = ijson.items(f, 'Siri.MonitoredVehicleJourney.item' )
            for r in responses:
                print(r)


            with open('{}{}.csv'.format(datadir,daily_filename), 'w') as csvfile:
                    # writer = csv.writer(csvfile)

                    headers=[
                        'service_date',
                        'trip_id',
                        'next_stop_id',
                        'next_stop_eta',
                        'next_stop_d_along_route',
                        'next_stop_d',
                        'vehicle_id',
                        'passenger_count'
                    ]

                    writer = csv.DictWriter(csvfile, fieldnames=headers,extrasaction='ignore')
                    writer.writeheader()
                    for response in responseGenerator:
                        writer.writerow(parse_bus(response))
                        sys.stdout.write('.')
    print ('finished at {}'.format(dt.datetime.now().strftime('%H-%M')))