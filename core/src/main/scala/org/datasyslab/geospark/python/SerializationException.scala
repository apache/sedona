package org.datasyslab.geospark.python

final case class SerializationException(private val message: String = "",
                                 private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
