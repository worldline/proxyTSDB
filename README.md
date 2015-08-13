# proxyTSDB
`v0.1.3-2`

The proxyTSDB is a local proxy to buffer metric sending between applications/tcollector plugins and openTSDB with these features :
* Offline-buffering
* Disk-persistence
* Generic 'gateway' (all apps / tcollector plugins send metrics to `localhost:4242`)

Its goal is to improve the collect of application and host metrics and add features which don't required application update.
## Configuration
	vi /etc/proxyTSDB/proxyTSDB.conf

	# Here you have to set your TSD collector service hostname
	TSD_HOST=<IP_or_HOSTNAME_of_openTSDB>
	
	#Interval in second between two sending to the openTSDB writer front
	SEND_TIME=30
	
	#LOG_LEVEL=[CRITICAL|ERROR|WARNING|INFO|DEBUG]
	LOG_LEVEL=INFO
	LOG_FILE=/var/log/proxyTSDB/proxyTSDB.log
	
	#PERSISTENCE_TYPE=[DISK|RAM]
	##RAM : Metrics are buffered only in RAM
	##DISK : Metrics are buffered in RAM and then stored to disk if openTSDB is unreachable or if the limit RAM_PERSISTENCE_MAX_SIZE is reached
	PERSISTENCE_TYPE=DISK
	DISK_QUEUE_PATH=/var/proxyTSDB/disk-queue-metrics
	
	#*_PERSISTENCE_MAX_SIZE=<integer_in_MegaByte>
	##Max size for the RAM buffer
	RAM_PERSISTENCE_MAX_SIZE=512
	##Max size for the DISK usage
	DISK_PERSISTENCE_MAX_SIZE=1024

* `SEND_TIME`: Interval in second between two sending to the openTSDB writer front.
* `PERSISTENCE_TYPE`:
	* `RAM`: Metrics are buffered only in RAM
	* `DISK`: Metrics are buffered in RAM and then stored to disk if openTSDB is unreachable or if the limit `RAM_PERSISTENCE_MAX_SIZE` is reached.
* `RAM_PERSISTENCE_MAX_SIZE`: Max size for the RAM buffer
* `DISK_PERSISTENCE_MAX_SIZE`: Max size for the DISK usage

## User Guide
To send a metric to the proxy, you just need to send it to 127.0.0.1:4242:
* Example for telnet or HTTP/REST:

		echo "put app.test 1434706884 50 pf=qualif" | nc 127.0.0.1 4242
* or

		curl -X POST --header "Content-Type: application/json" -d"{'metric':'app.test','timestamp':1434706884,'value':50,'tags':{'pf':'qualif'}}" "http://127.0.0.1:4242/api/put/"`

## API
### REST
* put
	* Add a metric
	* POST
	* `api/put `
	* data: `{'metric':'<metric_name>','timestamp':<timestamp>,'value':<num_value>,'tags:{'<tag_name>':'<tag_value>',...}}`
		* `metric` : The name of the metric you are storing
		* `timestamp` : A Unix epoch style timestamp in seconds or milliseconds. The timestamp must not contain non-numeric characters.
		* `value` : The value to record for this data point
		* `tags` : A map of tag name/tag value pairs. At least one pair must be supplied
* version
	* Request for the awl-proxyTSDB version
	* GET
	* `api/version`

### Telnet
* put

		put <metric> <timestamp> <value> <tagk1=tagv1[ tagk2=tagv2 ...tagkN=tagvN]>
* status

		status
Return the number of elements in RAM queue and (if used) DISK queue
* version

		version
