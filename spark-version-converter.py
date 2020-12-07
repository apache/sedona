# Python 3
import fileinput

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
        print('//    ' + line, end='')  # disable code
        return 'disabled'

def parse_file(filepath):
    conversion_result_spark2 = ''
    conversion_result_spark3 = ''
    with fileinput.FileInput(filepath, inplace=True) as file:
        for line in file:
            if spark2_anchor in line:
                conversion_result_spark2 = switch_version(line) + ' ' + spark2_anchor
            elif spark3_anchor in line:
                conversion_result_spark3 = switch_version(line) + ' ' + spark3_anchor
            else:
                print(line, end='')
        return conversion_result_spark2 + ' and ' + conversion_result_spark3

for filepath in files:
    print(filepath + ': ' + parse_file(filepath))