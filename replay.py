import os, bz2, time, json, sys, config

archive_list = sorted(os.listdir('raw'))

for archive in archive_list:
	try: 
		stream = bz2.BZ2File(os.path.join(os.getcwd(), os.path.join('raw',archive)))
		for tweet in stream:
			print tweet
	except Exception as e:
		print e

