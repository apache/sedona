package org.apache.sedona.common.simplify;

import org.locationtech.jts.geom.Coordinate;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class CoordinatesSimplifier {
    public static Coordinate[] simplifyInPlace(Coordinate[] geom, double tolerance, int minPointsExternal) {
        if (geom.length < 3 || geom.length <= minPointsExternal) {
            return geom;
        } else if (tolerance == 0 && minPointsExternal <= 2) {
            return ZeroToleranceGeometrySimplifier.simplifyInPlaceTolerance0(geom);
        } else {
            boolean[] kept_points = new boolean[geom.length];
            kept_points[0] = true;
            int itLast = geom.length - 1;
            kept_points[itLast] = true;
            int keptn = 2;
            Stack<Integer> iteratorStack = new Stack<Integer>();
            iteratorStack.push(0);
            int itFirst = 0;
            double toleranceSquared = tolerance * tolerance;
            double itTool = keptn >= minPointsExternal ? toleranceSquared : -1.0;

            while (!iteratorStack.isEmpty()){
                CoordinateSplitter.SplitInPlace splitInPlaceRes = CoordinateSplitter.splitInPlace(
                        geom,
                        itFirst,
                        itLast,
                        itTool
                );
                if (splitInPlaceRes.getSplit() == itFirst){
                    itFirst = itLast;
                    itLast = iteratorStack.pop();
                }
                else {
                    kept_points[splitInPlaceRes.getSplit()] = true;
                    keptn = keptn + 1;

                    iteratorStack.push(itLast);
                    itLast = splitInPlaceRes.getSplit();
                    itTool = keptn >= minPointsExternal ? toleranceSquared : -1.0;
                }
            }
            List<Coordinate> result = new ArrayList<>();
            for (int i = 0; i < kept_points.length; i++) {
                if (kept_points[i]) {
                    result.add(geom[i]);
                }
            }
            return result.toArray(new Coordinate[0]);
        }
    }
}
