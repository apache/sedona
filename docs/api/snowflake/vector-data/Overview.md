!!!note
    Please always keep the schema name `SEDONA` (e.g., `SEDONA.ST_GeomFromWKT`) when you use Sedona functions to avoid conflicting with Snowflake's built-in functions.

SedonaSQL supports SQL/MM Part3 Spatial SQL Standard. It includes four kinds of SQL operators as follows.

* Constructor: Construct a Geometry given an input string or coordinates
  * Example: ST_GeomFromWKT (string). Create a Geometry from a WKT String.
  * Documentation: [Here](Constructor.md)
* Function: Execute a function on the given column or columns
  * Example: ST_Distance (A, B). Given two Geometry A and B, return the Euclidean distance of A and B.
  * Documentation: [Here](Function.md)
* Aggregate function: Return the aggregated value on the given column
  * Example: ST_Envelope_Aggr (Geometry column). Given a Geometry column, calculate the entire envelope boundary of this column.
  * Documentation: [Here](AggregateFunction.md)
* Predicate: Execute a logic judgement on the given columns and return true or false
  * Example: ST_Contains (A, B). Check if A fully contains B. Return "True" if yes, else return "False".
  * Documentation: [Here](Predicate.md)
