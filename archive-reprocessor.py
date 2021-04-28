# archive-reprocessor.py
# 28 april 2021

#filenames like
# daily-2021-01-24.gz

#import pandas as pd
from fnmatch import fnmatch
import os
import datetime as dt
import tempfile

def get_daily_filelist(path):
	daily_filelist=[]
	include_list = ['daily*.gz']
	for dirname, _, filenames in os.walk(path):
		for filename in filenames:
			if any(fnmatch(filename, pattern) for pattern in include_list):
				daily_filelist.append(filename)
	sorted_daily_filelist = sorted(daily_filelist, key=lambda daily_filelist: daily_filelist[6:16])
	return sorted_daily_filelist

def get_route_filelist(temp_path):
	
	# daily_filelist=[]
	# include_list = ['daily*.gz']
	# for dirname, _, filenames in os.walk(path):
	# 	for filename in filenames:
	# 		if any(fnmatch(filename, pattern) for pattern in include_list):
	# 			daily_filelist.append(filename)
	# sorted_daily_filelist = sorted(daily_filelist, key=lambda daily_filelist: daily_filelist[6:16])
	# return sorted_daily_filelist


def unzip_daily_to_temp(path, temp_path,filename):

	return

# main loop
if __name__ == "main":

	path = os.getcwd()+'/data'

	dailies = get_daily_filelist(path)

	for daily_file in dailies:
		unzip_daily_to_temp(path, temp_path, daily_file)
		tf = tempfile.NamedTemporaryFile()

		# get list of file names in temp_path

		# for them 
			# unzip each of those to a temp path
			with tempfile.TemporaryDirectory() as tmpdirname:
			    print('Created temporary directory:', tmpdirname)


				# then parse in a df
			# concatenate dfs
			# does it delete folder automatically outside the with context handler?
		# concatenate dfs again
		# dump df to a CSV
