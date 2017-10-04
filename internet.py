import simplejson
import csv
import pandas as pd
import numpy as np
import google.auth
import time
from oauth2client.client import flow_from_clientsecrets
from oauth2client.client import GoogleCredentials
from google.cloud import bigquery


def write_csv(data,path):

	# with open(path,'w') as csv_file:
		# writer = csv.writer(csv_file)
		# writer.writerows(data)
	
	df = pd.DataFrame(data,columns=['Year','Count','Region','CSD Name','Mbps'])
	df.unstack(level=1)
	df.to_csv('mlabs_data.csv', index=False)
			
def wait_for_job(job):

	while True:
		job.reload()
		if job.state == 'DONE':
			if job.error_result:
				raise RuntimeError(job.errors)
			return
		time.sleep(1)

def format_data():

	data = pd.read_csv('mlabs_data.csv')
	names = data['CSD Name'].unique()
	output = pd.DataFrame(columns=['Year','CSD Name','Mbps'])

	for year in range (2009,2017):

		for city in names:
		
			print (year,city,end='\t\t\r')
		
			#extract data for year/city
			subset = data.loc[(data.Year == year) & (data['CSD Name'] == city)]
			
			#drop below 40th percentile, drop above 10th percentile. This is what speedtest.net does.
			#multiply values by 1.30, as MIT reports that mLabs underreports by about that much on average.
			thirty = subset.quantile(0.30).Mbps
			ninety = subset.quantile(0.90).Mbps
			
			#keep values within the 30th and 90th percentiles
			subset = subset[(subset.Mbps > thirty) & (subset.Mbps < ninety)]
			
			#get the average
			average = subset.mean()*1.30
			
			output.loc[len(output)] = [year,city,average.Mbps]
			
	output.to_csv('output.csv')		
		
def query_mlabs():

	download = ("""
		-- * 1000000
		SELECT
			STRFTIME_UTC_USEC(web100_log_entry.log_time * 1000000, '%Y') AS Year,
			COUNT(*) AS num_tests,
			connection_spec.client_geolocation.region AS region,
			connection_spec.client_geolocation.city AS city,
			CASE WHEN (8 * (web100_log_entry.snap.HCThruOctetsAcked / (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd)))>=0 THEN (8 * (web100_log_entry.snap.HCThruOctetsAcked / (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd))) END AS raw,
		FROM
			plx.google:m_lab.ndt.all
		WHERE
			connection_spec.client_geolocation.region == "AB"
			AND blacklist_flags == 0
			AND STRFTIME_UTC_USEC(web100_log_entry.log_time * 1000000, '%Y') < "2017"
		GROUP BY
			Year,
			city,
			region,
			raw
		ORDER BY
			Year ASC, city ASC;
		""")

	upload = ("""  
		-- * 1000000
		SELECT
			STRFTIME_UTC_USEC(web100_log_entry.log_time * 1000000, '%Y') AS Year,
			COUNT(*) AS num_tests,
			connection_spec.client_geolocation.region AS region,
			connection_spec.client_geolocation.city AS city,
			CASE WHEN (8 * (web100_log_entry.snap.HCThruOctetsReceived / web100_log_entry.snap.Duration))>=0.1 THEN (8 * (web100_log_entry.snap.HCThruOctetsReceived / web100_log_entry.snap.Duration)) END AS raw,
		FROM
			plx.google:m_lab.ndt.all
		WHERE
			connection_spec.client_geolocation.region == "AB"
			AND blacklist_flags == 0
			AND STRFTIME_UTC_USEC(web100_log_entry.log_time * 1000000, '%Y') < "2017"
		GROUP BY
			Year,
			city,
			region,
			raw
		ORDER BY
			Year ASC;
		""")

	query = download ##make this loop and do both?##
		
	client = bigquery.Client('m-labs-internet-data')
	query_job = client.run_async_query(job_name='download_speed'+time.strftime('%H%M%S'),query=query)
	query_job.use_legacy_sql = True
	
	print ('Starting query')
	
	query_job.begin()
	
	wait_for_job(query_job)
	
	#Request one page at a time
	query_results = query_job.results()
	page_token = None

	repository = []
	
	count = 0
	max = 10000
	
	while True:
		rows, total_rows, page_token = query_results.fetch_data(
			max_results=max,
			page_token=page_token)

		for row in rows:
			repository.append(row)
			print(count, ' Appending Data',row,end='\t\t\r')

		if not page_token:
			break
		
		count += 1*max
		
	path = 'mlabs_upload_data.csv'
	write_csv(repository,path)
	
if __name__ == '__main__':
	query_mlabs()
	format_data()