sedona_jar_files () {
  local subdir
  for subdir in 'core' 'sql' 'viz'; do
    local artifact_id="$(
      mvn \
        org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate \
        -Dexpression=project.artifactId \
        -q \
        -DforceStdout \
        -f "${subdir}"/target/resolved-pom.xml
    )"
    local artifact_version="$(
      mvn \
        org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate \
        -Dexpression=project.version \
        -q \
        -DforceStdout \
        -f "${subdir}"/target/resolved-pom.xml
    )"
    echo "$(pwd)/${subdir}/target/${artifact_id}-${artifact_version}.jar"
  done
}

export SEDONA_JAR_FILES="$(sedona_jar_files | paste -sd':')"
