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
import os
import sys
import yaml                     #to read configuration from yaml file
import argparse                 #argument parser to parse command line arguments

#imports from 3rd party libraries
from huawei_solar import (
    create_device_instance,
    create_tcp_client,
)
from huawei_solar import RegisterName
#from huawei_solar import register_names as reg
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


async def get_solar_data(registers):
    """get_solar_data connect to inverter, read registers and write data to influxdb

    Args:
        registers (huawei_solar.register_names): modbus registers to read
    """

    device = None
    client = None

    sys.stdout.write("Starting solar data collection loop...\n")
    try:
        while True:
            # 1. Establish connection if it does not exist (or was lost)
            if device is None:
                try:
                    client = create_tcp_client(
                        host=SUN2000_HOST, 
                        port=SUN2000_PORT, 
                        unit_id=SUN2000_SLAVE_ID, 
                        timeout=SUN2000_TIMEOUT
                    )
                    device = await create_device_instance(client)
                    sys.stdout.write(f'Connected to Huawei SUN2000 inverter, host: {SUN2000_HOST}:{SUN2000_PORT} (unit ID: {SUN2000_SLAVE_ID})\n')
                except Exception as conn_err:
                    sys.stderr.write(f'Connection to Huawei SUN2000 inverter, host: {SUN2000_HOST}:{SUN2000_PORT} (unit ID: {SUN2000_SLAVE_ID}) failed: {conn_err}. Retrying in next cycle...\n')
                    device = None  # Reset connection state

            # 2. Fetch and write data (only if connected)
            if device is not None:
                try:
                    data = []
                    results = await device.batch_update(registers)
                    serial_entry = results.get(RegisterName("serial_number"))
                    inverter_serial_number = serial_entry.value if serial_entry else "UNKNOWN"

                    for register, result in results.items():
                        sys.stdout.write(f'Read register "{register}": {result.value} {result.unit}\n')
                        if register != 'serial_number':
                            ms = {}
                            # Use time.time_ns() if strftime('%s') causes issues on your OS
                            ms["time"] = int(datetime.datetime.now().strftime('%s')) * 10**9
                            ms["measurement"] = register
                            ms["fields"] = {"value": result.value}
                            ms["tags"] = {"unit": result.unit, "serialnumber": inverter_serial_number}
                            if result.value > 0:
                                data.append(ms)

                    # InfluxDB write process
                    if INFLUX_DB_VERSION == 1:
                        dbclient_v1.write_points(data)
                    elif INFLUX_DB_VERSION == 2:
                        write_api.write(bucket=INFLUX_V2_BUCKET, org=INFLUX_V2_ORG, record=data)

                except Exception as cycle_err:
                    # Catches Modbus timeouts or InfluxDB network errors
                    sys.stderr.write(f'Error during read/write cycle: {cycle_err}\n')
                    
                    # Proactively reset the connection for the next cycle
                    try:
                        await device.stop()
                    except Exception:
                        pass
                    device = None

            # 3. Wait before the next execution cycle
            sys.stdout.write(f'Going to sleep for {READ_INTERVAL} seconds.\n')
            if READ_INTERVAL == 0:
                break  # One-shot execution (e.g. triggered by Cronjob or Systemd timer)
            else:
                await asyncio.sleep(READ_INTERVAL)

    except KeyboardInterrupt:
        sys.stdout.write('Terminating program on user request (KeyboardInterrupt).\n')
    finally:
        # Final cleanup on script termination
        if device is not None:
            try:
                await device.stop()
                sys.stdout.write('Connection to Huawei SUN2000 inverter closed.\n')
            except Exception as e:
                sys.stderr.write(f'Warning: Error closing device connection: {e}\n')


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

# ***************** MAIN PROGRAM EXECUTION ***************** #
if __name__ == "__main__":

    if INFLUX_DB_VERSION == 1:
        if INFLUX_V1_PASSWORD is None:
            INFLUX_V1_PASSWORD = read_secret('INFLUX_V1_PASSWORD')
        dbclient_v1 = InfluxDBClient_v1(
            INFLUX_V1_HOST,
            database=INFLUX_V1_DB,
            username=INFLUX_V1_USERNAME,
            password=INFLUX_V1_PASSWORD
            )
        try:
            asyncio.run(get_solar_data(SUN2000_REGISTERS))
        finally:
            if dbclient_v1 is not None:
                try:
                    dbclient_v1.close()
                    sys.stdout.write('InfluxDB client closed.\n')
                except Exception as e:
                    sys.stderr.write(f'Warning: Error closing DB client: {e}\n')

    elif INFLUX_DB_VERSION == 2:
        # Hier mit dem Context-Manager 'with' arbeiten
        with InfluxDBClient_v2(
            url=INFLUX_V2_URL,
            token=INFLUX_V2_TOKEN,
            org=INFLUX_V2_ORG
            ) as dbclient_v2:
            write_api = dbclient_v2.write_api(write_options=SYNCHRONOUS)

            try:
                asyncio.run(get_solar_data(SUN2000_REGISTERS))
            finally:
                if write_api is not None:
                    try:
                        write_api.close()
                        sys.stdout.write('WriteApi closed.\n')
                    except Exception as e:
                        sys.stderr.write(f'Warning: Error closing WriteApi: {e}\n')

    else:
        sys.stderr.write('Error: No valid InfluxDB version given in configuration.\n')
        exit(1)
