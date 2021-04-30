# archive-reprocessor.py
# 28 april 2021

from fnmatch import fnmatch
import os
import gzip
import shutil
import sys
from json_stream_parser import load_iter
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


if __name__ == "__main__":

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
			sys.stdout.write('Parsing JSON responses and dumping to db.')
			with open(ungzipfile, 'r') as f:
				responseGenerator = (r for r in load_iter(f))
				db_url=get_db_url()
				create_table(db_url)
				session = get_session()
				for response in responseGenerator:
					sys.stdout.write('.')
					bus = parse_buses(response, db_url)
					session.add(bus)
				session.commit()

