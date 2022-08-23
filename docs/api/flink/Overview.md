# Introduction

SedonaSQL supports SQL/MM Part3 Spatial SQL Standard. Please read the programming guide: [Sedona with Flink SQL app](../../tutorial/flink/sql.md).

Sedona includes SQL operators as follows.

* Constructor: Construct a Geometry given an input string or coordinates
    * Example: ST_GeomFromWKT (string). Create a Geometry from a WKT String.
* Function: Execute a function on the given column or columns
    * Example: ST_Distance (A, B). Given two Geometry A and B, return the Euclidean distance of A and B.
* Aggregator: Return a single aggregated value on the given column
	* Example: ST_Envelope_Aggr (Geometry column). Given a Geometry column, calculate the entire envelope boundary of this column.
* Predicate: Execute a logic judgement on the given columns and return true or false
    * Example: ST_Contains (A, B). Check if A fully contains B. Return "True" if yes, else return "False".
