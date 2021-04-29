# archive-reprocessor.py
# 28 april 2021

#filenames like
# daily-2021-01-24.gz

#import pandas as pd
from fnmatch import fnmatch
import os
import gzip
import shutil
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


def unzip_to_file(infile, outfile):
	with gzip.open(infile, 'r') as f_in, open(outfile, 'wb') as f_out:
		shutil.copyfileobj(f_in, f_out)
		return



# main loop
if __name__ == "__main__":

	datadir = os.getcwd()+'/data/'
	print(datadir)

	dailies = get_daily_filelist(datadir)

	for daily_filename in dailies:
		print ('reading {}{}'.format(datadir,daily_filename))

		# decompress the file to temp dir
		with tempfile.TemporaryDirectory() as temp_dir:
			infile = datadir+daily_filename
			outfile = temp_dir + '{}.json'.format(daily_filename)
			unzip_to_file(infile, outfile)
			print ('Decompressed {} to {}'.format(infile,outfile))

			with open(outfile, 'r', encoding='utf-8') as f:
				print('opening {}'.format(outfile))
				char = f.read(100)
				print (char)
