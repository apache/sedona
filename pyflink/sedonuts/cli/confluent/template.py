function_template = """
CREATE FUNCTION {} AS 'org.apache.sedona.flink.confluent.{}.{}' USING JAR 'confluent-artifact://{}';
""".strip()
