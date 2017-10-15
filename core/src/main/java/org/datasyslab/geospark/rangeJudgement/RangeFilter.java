/**
 * FILE: RangeFilter.java
 * PATH: org.datasyslab.geospark.rangeJudgement.RangeFilter.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.rangeJudgement;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.spark.api.java.function.Function;


// TODO: Auto-generated Javadoc

public class RangeFilter<U extends Geometry, T extends Geometry> extends JudgementBase implements Function<T, Boolean> {

	public RangeFilter(U queryWindow, boolean considerBoundaryIntersection) {
		super(queryWindow, considerBoundaryIntersection);
	}

	/* (non-Javadoc)
         * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
         */
	public Boolean call(T geometry) throws Exception {
		return match(geometry, queryGeometry);
	}
}
