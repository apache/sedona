# SedonaSnow

SedonaSnow is a plugin that brings the open-source Apache Sedona functions to Snowflake. The plugin doubles the spatial SQL functions supported by Snowflake, and hence enables snowflake users to do more with their geospatial data.

# Why Integrate Apache Sedona with Snowflake?

Apache Sedona is a powerful open-source library for large-scaling geospatial data processing and analysis. It has been used in use cases where data engineers need to process spatial data at scale in their complicated data stack. On the other hand, Snowflake is a cloud-based data warehousing platform that allows for seamless data integration and processing.

Many Snowflake users collect and store geospatial data in the platform. To meet this need, Snowflake released the GEOGRAPHY data type and corresponding geospatial functions in June 2021. Since then, new features have been added to fulfill computation demands.

The Wherobots team has observed a significant overlap in user bases between Snowflake and Sedona. For users who have been utilizing Sedona Spatial SQL APIs, we aim to enable them to continue using their favorite Sedona-flavored ST functions through Snowflake SQL.