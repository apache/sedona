function_template = """
CREATE FUNCTION {} AS 'org.apache.sedona.flink.confluent.constructors.{}' USING JAR 'confluent-artifact://{}';
""".strip()
