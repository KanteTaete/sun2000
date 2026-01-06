# Sun2000: Huawei SUN2000 inverter data reader
# Copyright (C) 2023-2026  Roel Huybrechts, KanteTaete

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

# imports from standard libraries
import asyncio
import datetime
import time
import os
import sys
import atexit                   #to call functions on exit of script
import yaml                     #to read configuration from yaml file
import argparse                 #argument parser to parse command line arguments

#imports from 3rd party libraries
from huawei_solar import AsyncHuaweiSolar
# Lib for InfluxDB v1
from influxdb import InfluxDBClient as InfluxDBClient_v1
# Lib for InfluxDB v2
from influxdb_client.client.influxdb_client import InfluxDBClient as InfluxDBClient_v2
from influxdb_client.client.write_api import SYNCHRONOUS


# derive command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("-c", "--configfile", 
                    default = './etc/sun2000.yaml',
                    help="path to configuration file (default: ./etc/sun2000.yaml)")
args = parser.parse_args()

# read configuration from args.configfile (default: ./etc/sun2000.yaml)
try:
    with open(args.configfile, 'r') as config_file:
        config = yaml.safe_load(config_file)
except FileNotFoundError:
    sys.stderr.write(f'Error: Configuration file "{args.configfile}" not found.\n')
    exit(1)
except yaml.YAMLError as e:
    sys.stderr.write(f'Error: YAML error while reading configuration file "{args.configfile}": {e}\n')
    exit(1)


#***************** function definition start *****************#

def read_secret(variable_name):
    """read_secret reads secret from a file 
    if environment variable <variable_name>_FILE is set,
    otherwise from environment variable <variable_name>.

    Args:
        variable_name (str): name of environment variable or file name containing the secret

    Returns:
        str: secret value
    """
    if f'{variable_name}_FILE' in os.environ:
        with open(str(os.environ.get(f'{variable_name}_FILE')), 'r') as secret_file:
            secret = secret_file.read()
    else:
        secret = str(os.environ.get(variable_name, None))
    return secret

def on_exit(db_client, write_api):
    """on_exit Close clients after terminate a script.

    :param db_client: InfluxDB client
    :param write_api: WriteApi  - only for InfluxDB v2
    :return: nothing
    """
    if write_api is not None:
        write_api.close()
    db_client.close()

#***************** function definition end *****************#

#***************** configuration variables (constants) definition start *****************#

INFLUX_DB_VERSION = config['influx']['influx_db_version']

# variables for InfluxDB version v1
INFLUX_V1_HOST = config['influx']['influx_v1_host']
INFLUX_V1_DB = config['influx']['influx_v1_db']
INFLUX_V1_USERNAME = config['influx']['influx_v1_username']
INFLUX_V1_PASSWORD = config['influx']['influx_v1_password']

# variables for InfluxDB version v2
INFLUX_V2_URL = config['influx']['influx_v2_url']
INFLUX_V2_BUCKET = config['influx']['influx_v2_bucket']
INFLUX_V2_ORG = config['influx']['influx_v2_org']
INFLUX_V2_TOKEN = config['influx']['influx_v2_token']


# variables for Huawei SUN2000 inverter
SUN2000_HOST = config['inverter']['sun2000_host']
SUN2000_PORT = config['inverter']['sun2000_port']
SUN2000_SLAVE_ID = config['inverter']['sun2000_slave_id']
SUN2000_TIMEOUT = config['inverter']['sun2000_timeout']
SUN2000_REGISTERS = config['inverter']['sun2000_registers']


# general variables
READ_INTERVAL = config['general']['read_interval']

#***************** configuration variables definition end *****************#


if INFLUX_DB_VERSION == 1:
    if INFLUX_V1_PASSWORD is None:
        INFLUX_V1_PASSWORD = read_secret('INFLUX_V1_PASSWORD')
    dbclient_v1 = InfluxDBClient_v1(INFLUX_V1_HOST, database=INFLUX_V1_DB,
                          username=INFLUX_V1_USERNAME, password=INFLUX_V1_PASSWORD)
elif INFLUX_DB_VERSION == 2:
    dbclient_v2 = InfluxDBClient_v2(url=INFLUX_V2_URL, token=INFLUX_V2_TOKEN,org=INFLUX_V2_ORG)
    write_api=dbclient_v2.write_api(write_options=SYNCHRONOUS)
else:
    sys.stderr.write('Error: No InfluxDB version given in environement variable "INFLUX_DB_VERSION" (file "environment.env").')
    exit(1)


async def get_solar_data(registers):
    """get_solar_data connect to inverter, read registers and write data to influxdb

    Args:
        registers (huawei_solar.register_names): modbus registers to read
    """
    conn = await AsyncHuaweiSolar.create(host=SUN2000_HOST, port=SUN2000_PORT, slave_id=SUN2000_SLAVE_ID, timeout=SUN2000_TIMEOUT)
    sys.stdout.write(f'Connected to Huawei SUN2000 inverter, host: {SUN2000_HOST}:{SUN2000_PORT} (slave ID: {SUN2000_SLAVE_ID})\n')
    
    # sleep 2 seconds to ensure connection is established
    time.sleep(2)

    try:
        while True:
            data = []

            for r in registers:
                ms = {}
                ms["time"] = int(datetime.datetime.now().strftime('%s')) * 10**9
                result = await conn.get(name=r,slave_id=SUN2000_SLAVE_ID)

                ms["measurement"] = r
                ms["fields"] = {"value": result.value}
                ms["tags"] = {"unit": result.unit}
                sys.stdout.write(f'Read register "{r}": {result.value} {result.unit}\n')
                if result.value > 0:
                    data.append(ms)

            if INFLUX_DB_VERSION == 1:
                dbclient_v1.write_points(data)
            elif INFLUX_DB_VERSION == 2:
                write_api.write(bucket=INFLUX_V2_BUCKET, org=INFLUX_V2_ORG, record=data)

            sys.stdout.write(f'Going to sleep for {READ_INTERVAL} seconds.\n')
            time.sleep(READ_INTERVAL)
    except KeyboardInterrupt:
        sys.stdout.write('Terminating program on user request (KeyboardInterrupt).\n')
    except Exception as e:
        # likely catches all exceptions within while loop --> add extra try/except where reading registers and
        # where writing to influxdb; implementation pending
        sys.stderr.write(f'Error: Exception occurred: {e}\n')
    finally:
        # close connection to inverter
        await conn.stop()
        sys.stdout.write('Connection to Huawei SUN2000 inverter closed.\n')

asyncio.run(get_solar_data(SUN2000_REGISTERS))


# call after terminate a script
if INFLUX_DB_VERSION == 1:
    atexit.register(on_exit, dbclient_v1, None)
elif INFLUX_DB_VERSION == 2:
    atexit.register(on_exit, dbclient_v2, write_api)