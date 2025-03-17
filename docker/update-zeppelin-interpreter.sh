#!/bin/sh

apt-get install -y jq

# Find the latest Sedona and GeoTools JARs
SEDONA_JAR=$(find "$SPARK_HOME/jars/" -name "sedona-spark-shaded-*.jar" | head -n 1)
GEOTOOLS_JAR=$(find "$SPARK_HOME/jars/" -name "geotools-wrapper-*.jar" | head -n 1)

# Ensure we found the jars
if [ -z "$SEDONA_JAR" ] || [ -z "$GEOTOOLS_JAR" ]; then
    echo "Error: One or both JARs not found."
    exit 1
fi

# Update interpreter.json using jq
jq --arg sedona "$SEDONA_JAR" --arg geotools "$GEOTOOLS_JAR" '
  .interpreterSettings.spark.dependencies |= map(
    if .groupArtifactVersion | test("sedona-spark-shaded-.*\\.jar$") then
      .groupArtifactVersion = $sedona
    elif .groupArtifactVersion | test("geotools-wrapper-.*\\.jar$") then
      .groupArtifactVersion = $geotools
    else . end
  )
' "$ZEPPELIN_HOME/conf/interpreter.json" > "$ZEPPELIN_HOME/conf/interpreter.json.tmp" && \
mv "$ZEPPELIN_HOME/conf/interpreter.json.tmp" "$ZEPPELIN_HOME/conf/interpreter.json"

echo "Updated interpreter.json successfully!"