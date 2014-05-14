package org.khelekore.prtree;

import java.io.Serializable;

/** One implementatoin of a point
 */
public class SimplePointND implements PointND, Serializable {
    private static final long serialVersionUID = 8800395426991319858L;
    private final double[] ords;

    /** Create a new SimplePointND using the given ordinates.
     * @param ords the ordinates
     */
    public SimplePointND (double... ords) {
	this.ords = ords;
    }

    public int getDimensions () {
	return ords.length;
    }

    public double getOrd (int axis) {
	return ords[axis];
    }
}
