import time
import datetime
import psutil
from influxdb import InfluxDBClient

total_run_seconds = 30 * 60

json_body = [
    {
        "measurement": "cpu_load_per_sec",
        "tags": {
            "host": "freypc",
            "region": "shanghai"
        },
        "time": "",
        "fields": {
            "value": 0
        }
    }
]


class InfluxInserter(object):
    def __init__(self):
        self.db_client = InfluxDBClient('localhost', 8086, 'root', 'root', 'test')
        pass

    def run_insert(self):
        while total_run_seconds > 0:
            try:
                usage = psutil.cpu_percent()
                utc_dt = datetime.datetime.utcfromtimestamp(time.time())
                json_body[0]["fields"]["value"] = usage
                json_body[0]["time"] = str(utc_dt)
                self.db_client.write_points(json_body)
                time.sleep(1)
                print json_body
            except Exception as e:
                raise e



if __name__ == '__main__':
    influx_inserter = InfluxInserter()
    influx_inserter.run_insert()


