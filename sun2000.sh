#!/bin/bash

# Sun2000: Huawei SUN2000 inverter data reader
# Copyright (C) 2023-2024  Roel Huybrechts

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

source /opt/sun2000/venv/bin/activate

set -a
source /opt/sun2000/environment.env
set +a

python /opt/sun2000/read_data.py -c "/opt/sun2000/etc/sun2000.yaml"