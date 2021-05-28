# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at

#   http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Python 3
import fileinput
import sys

spark2_anchor = 'SPARK2 anchor'
spark3_anchor = 'SPARK3 anchor'
files = ['sql/src/main/scala/org/apache/sedona/sql/UDF/UdfRegistrator.scala',
         'sql/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/TraitJoinQueryExec.scala',
         'sql/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/JoinQueryDetector.scala',
         'sql/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/BroadcastIndexJoinExec.scala',
         'sql/src/main/scala/org/apache/spark/sql/sedona_sql/io/GeotiffFileFormat.scala']

def switch_version(line):
    if line[:2] == '//':
        print(line[2:], end='')  # enable code
        return 'enabled'
    else:
        print('//' + line, end='')  # disable code
        return 'disabled'

def enable_version(line):
    if line[:2] == '//':
        print(line[2:], end='')  # enable code
        return 'enabled'
    else:
        print(line, end='')
        return 'enabled before'

def disable_version(line):
    if line[:2] == '//':
        print(line, end='')
        return 'disabled before'
    else:
        print('//' + line, end='')  # disable code
        return 'disabled'

def parse_file(filepath, argv):
    conversion_result_spark2 = ''
    conversion_result_spark3 = ''
    if argv[1] == 'spark2':
        with fileinput.FileInput(filepath, inplace=True) as file:
            for line in file:
                if spark2_anchor in line:
                    conversion_result_spark2 = spark2_anchor + ' ' + enable_version(line)
                elif spark3_anchor in line:
                    conversion_result_spark3 = spark3_anchor + ' ' + disable_version(line)
                else:
                    print(line, end='')
            return conversion_result_spark2 + ' and ' + conversion_result_spark3
    elif argv[1] == 'spark3':
        with fileinput.FileInput(filepath, inplace=True) as file:
            for line in file:
                if spark2_anchor in line:
                    conversion_result_spark2 = spark2_anchor + ' ' + disable_version(line)
                elif spark3_anchor in line:
                    conversion_result_spark3 = spark3_anchor + ' ' + enable_version(line)
                else:
                    print(line, end='')
            return conversion_result_spark2 + ' and ' + conversion_result_spark3
    else:
        return 'wrong spark version'

for filepath in files:
    print(filepath + ': ' + parse_file(filepath, sys.argv))