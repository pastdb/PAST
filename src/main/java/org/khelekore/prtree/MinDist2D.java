package org.khelekore.prtree;

import java.io.Serializable;

/** Class that can calculate the MINDIST between a point and a rectangle
 */
public class MinDist2D implements Serializable {
    private static final long serialVersionUID = 7420866523588447094L;

    /** Do not instantiate
     */
    private MinDist2D () {
	// empty
    }

    /** Calculate the MINDIST between the given rectangle and the given point
     * @param minx the rectangle minimum x point
     * @param miny the rectangle minimum y point
     * @param maxx the rectangle maximum x point
     * @param maxy the rectangle maximum y point
     * @param x the point
     * @param y the point
     * @return the squared distance
     */
    public static double get (double minx, double miny,
			      double maxx, double maxy,
			      double x, double y) {
	double rx = r (x, minx, maxx);
	double ry = r (y, miny, maxy);
	double xd = x - rx;
	double yd = y - ry;
	return xd * xd + yd * yd;	
    }

    private static double r (double x, double min, double max) {
	double r = x;
	if (x < min)
	    r = min;
	if (x > max)
	    r = max;
	return r;
    }
}
