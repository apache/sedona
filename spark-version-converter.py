# Python 3
import fileinput
import sys

spark2_anchor = 'SPARK2 anchor'
spark3_anchor = 'SPARK3 anchor'
files = ['sql/src/main/scala/org/apache/sedona/sql/UDF/UdfRegistrator.scala',
         'sql/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/TraitJoinQueryExec.scala',
         'sql/src/main/scala/org/apache/spark/sql/sedona_sql/strategy/join/JoinQueryDetector.scala']

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