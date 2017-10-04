# mlabsInternetQuery
Pull internet speed test data from M-Lab on Google BigQuery.

You will need to setup Google Cloud SDK on your machine.

To change your region, simply swap out "AB" to your preferred region's abbreviation.
```
75| connection_spec.client_geolocation.region == "AB"
```
Adjust the timeframe by changing this line:
```
77| AND STRFTIME_UTC_USEC(web100_log_entry.log_time * 1000000, '%Y') < "2017"
```
