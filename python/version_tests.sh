current_dir=$PWD
file_name=$current_dir/"spark_versions_config"
rm  $current_dir/tests_done.txt
pipenv --rm
cat $file_name | while read LINE; do
    export SPARK_HOME=$LINE
    echo $SPARK_HOME
    py4j_version=$(ls $SPARK_HOME/python/lib/ -t -U | grep -m 1 py4j)
    export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/$py4j_version
    echo Testing Spark
    file=$( echo ${LINE##/*/} )
    echo $file >> $current_dir/tests_done.txt
    echo $file
    pipenv lock --clear
    pipenv install -d
    pipenv run pip freeze | grep pyspark >> $current_dir/tests_done.txt
    geo_pyspark/tests/run_tests.sh
done
