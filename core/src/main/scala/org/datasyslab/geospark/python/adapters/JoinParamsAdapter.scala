package org.datasyslab.geospark.python.adapters


import org.datasyslab.geospark.enums.{IndexType, JoinBuildSide}
import org.datasyslab.geospark.spatialOperator.JoinQuery.JoinParams

object JoinParamsAdapter {
  def createJoinParams(useIndex: Boolean = false, indexType: String, joinBuildSide: String): JoinParams = {
    val buildSide = JoinBuildSide.getBuildSide(joinBuildSide)
    val currIndexType = IndexType.getIndexType(indexType)
    new JoinParams(useIndex, currIndexType, buildSide)
  }
}