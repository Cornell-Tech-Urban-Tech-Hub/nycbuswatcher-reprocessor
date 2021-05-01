# archive-reprocessor.py
# 30 april 2021

# todo add logic for mysql dump too
# todo add logic to dump entire stream to a single sqlite file
# todo add logic to dump to a CSV

from fnmatch import fnmatch
import os
import gzip
import shutil
import sys
import ijson
from Database import *

def get_daily_filelist(path):
	daily_filelist=[]
	include_list = ['daily*.gz']
	for dirname, _, filenames in os.walk(path):
		for filename in filenames:
			if any(fnmatch(filename, pattern) for pattern in include_list):
				daily_filelist.append(filename)
	sorted_daily_filelist = sorted(daily_filelist, key=lambda daily_filelist: daily_filelist[6:16])
	return sorted_daily_filelist


def db_init(daily_filename):
	daily_filename=daily_filename[:-3] #remove .gz
	db_url = get_db_url(daily_filename)
	create_table(db_url)
	session = get_session(db_url)
	return session

# after https://www.aylakhan.tech/?p=27
def extract_responses(f):
	responses = ijson.items(f, 'Siri', multiple_values=True)
	for response in responses:
		yield response


if __name__ == "__main__":

	datadir = os.getcwd()+'/data/'
	dailies = get_daily_filelist(datadir)

	for daily_filename in dailies:

		print('started at {}'.format(datetime.datetime.now()))

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
			sys.stdout.write('Parsing JSON responses and dumping to db.')
			with open(ungzipfile, 'r') as f:
				session = db_init(daily_filename)
				for siri_response in extract_responses(f):
				# for siri_response in load_iter(f):
					buses = parse_buses(siri_response)
					for bus in buses:
						session.add(bus)
						# sys.stdout.write('.') # <-- is this the big slowdown?
					session.commit() # if too slow, de-indent me?

			print ('finished at {}'.format(datetime.datetime.now()))

