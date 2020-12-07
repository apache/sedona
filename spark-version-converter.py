# Python 3
import fileinput

spark2_anchor='SPARK2'
spark3_anchor='SPARK3'

def switch_version(line):
    if '//Catalog' in line:
        print(line.replace('//Catalog', 'Catalog'), end='')  # enable code
        return 'enabled'
    else:
        print(line.replace('Catalog', '//Catalog'), end='')  # disable code
        return 'disabled'
def parse_file():
    conversion_result_spark2 = ''
    conversion_result_spark3 = ''
    with fileinput.FileInput("sql/src/main/scala/org/apache/sedona/sql/UDF/UdfRegistrator.scala", inplace=True) as file:
        for line in file:
            if spark2_anchor in line:
                conversion_result_spark2 = switch_version(line) + ' ' + spark2_anchor
            elif spark3_anchor in line:
                conversion_result_spark3 = switch_version(line) + ' ' + spark3_anchor
            else:
                print(line, end='')
        return conversion_result_spark2 + ' and ' + conversion_result_spark3

print(parse_file())