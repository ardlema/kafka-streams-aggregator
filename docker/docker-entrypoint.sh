#!/bin/bash -xe
#
# © 2017 Stratio Big Data Inc., Sucursal en España.
#
# This software is licensed under the Apache 2.0.
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the terms of the License for more details.
#
# SPDX-License-Identifier:  Apache-2.0.
#

java -jar /aggregator.jar $BROKER_LIST $ZOOKEEPER $SCHEMA_REGISTRY $INPUT_TOPIC $OUTPUT_TOPIC

tail -F /aggregator.log