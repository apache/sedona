package org.datasyslab.geospark.python.adapters

import com.vividsolutions.jts.geom.Envelope
import net.razorvine.pickle.Unpickler
import net.razorvine.pickle.objects.ClassDict
import scala.collection.JavaConverters._

object EnvelopeAdapter {
  def getFromPython(bytes: Array[Byte]): java.util.List[Envelope] = {
    val arrBytes = bytes.map(x => x.toByte)
    val unpickler = new Unpickler
    val pythonEnvelopes = unpickler.loads(arrBytes).asInstanceOf[java.util.ArrayList[_]].toArray
    pythonEnvelopes.map(pythonEnvelope => new Envelope(
      pythonEnvelope.asInstanceOf[ClassDict].get("minx").asInstanceOf[Double],
      pythonEnvelope.asInstanceOf[ClassDict].get("maxx").asInstanceOf[Double],
      pythonEnvelope.asInstanceOf[ClassDict].get("miny").asInstanceOf[Double],
      pythonEnvelope.asInstanceOf[ClassDict].get("maxy").asInstanceOf[Double]
    )).toList.asJava
  }
}
