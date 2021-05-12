SEDONA_JAR_VERSION='1.0.1'

export SEDONA_JAR_FILES="$(
  paste -sd':' <<EOF
$(pwd)/core/target/sedona-core-${SPARK_VERSION:0:3}_${SCALA_VERSION:0:4}-${SEDONA_JAR_VERSION}-incubating-SNAPSHOT.jar
$(pwd)/sql/target/sedona-sql-${SPARK_VERSION:0:3}_${SCALA_VERSION:0:4}-${SEDONA_JAR_VERSION}-incubating-SNAPSHOT.jar
$(pwd)/viz/target/sedona-viz-${SPARK_VERSION:0:3}_${SCALA_VERSION:0:4}-${SEDONA_JAR_VERSION}-incubating-SNAPSHOT.jar
EOF
)"
