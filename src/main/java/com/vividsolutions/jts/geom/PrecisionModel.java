

/*
 * The JTS Topology Suite is a collection of Java classes that
 * implement the fundamental operations required to validate a given
 * geo-spatial data set to a known topological specification.
 *
 * Copyright (C) 2001 Vivid Solutions
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * For more information, contact:
 *
 *     Vivid Solutions
 *     Suite #1A
 *     2328 Government Street
 *     Victoria BC  V8T 5G5
 *     Canada
 *
 *     (250)385-6040
 *     www.vividsolutions.com
 */
package com.vividsolutions.jts.geom;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Specifies the precision model of the {@link Coordinate}s in a {@link Geometry}.
 * In other words, specifies the grid of allowable
 *  points for all <code>Geometry</code>s.
 * <p>
 * The {@link #makePrecise(Coordinate)} method allows rounding a coordinate to
 * a "precise" value; that is, one whose
 *  precision is known exactly.
 *<p>
 * Coordinates are assumed to be precise in geometries.
 * That is, the coordinates are assumed to be rounded to the
 * precision model given for the geometry.
 * JTS input routines automatically round coordinates to the precision model
 * before creating Geometries.
 * All internal operations
 * assume that coordinates are rounded to the precision model.
 * Constructive methods (such as boolean operations) always round computed
 * coordinates to the appropriate precision model.
 * <p>
 * Currently three types of precision model are supported:
 * <ul>
 * <li>FLOATING - represents full double precision floating point.
 * This is the default precision model used in JTS
 * <li>FLOATING_SINGLE - represents single precision floating point.
 * <li>FIXED - represents a model with a fixed number of decimal places.
 *  A Fixed Precision Model is specified by a scale factor.
 *  The scale factor specifies the size of the grid which numbers are rounded to.
 *  Input coordinates are mapped to fixed coordinates according to the following
 *  equations:
 *    <UL>
 *      <LI> jtsPt.x = round( (inputPt.x * scale ) / scale
 *      <LI> jtsPt.y = round( (inputPt.y * scale ) / scale
 *    </UL>
 * </ul>
 * For example, to specify 3 decimal places of precision, use a scale factor
 * of 1000. To specify -3 decimal places of precision (i.e. rounding to
 * the nearest 1000), use a scale factor of 0.001.
 * <p>
 * Coordinates are represented internally as Java double-precision values.
 * Since Java uses the IEEE-394 floating point standard, this
 * provides 53 bits of precision. (Thus the maximum precisely representable
 * <i>integer</i> is 9,007,199,254,740,992 - or almost 16 decimal digits of precision).
 * <p>
 * JTS binary methods currently do not handle inputs which have different precision models.
 * The precision model of any constructed geometric value is undefined.
 *
 *@version 1.7
 */
public class PrecisionModel implements Serializable, Comparable
{
	/**
	 * Determines which of two {@link PrecisionModel}s is the most precise
	 * (allows the greatest number of significant digits).
	 * 
	 * @param pm1 a PrecisionModel
	 * @param pm2 a PrecisionModel
	 * @return the PrecisionModel which is most precise
	 */
	public static PrecisionModel mostPrecise(PrecisionModel pm1, PrecisionModel pm2)
	{
		if (pm1.compareTo(pm2) >= 0)
			return pm1;
		return pm2;
	}
	
  private static final long serialVersionUID = 7777263578777803835L;

  /**
   * The types of Precision Model which JTS supports.
   */
  public static class Type
      implements Serializable
  {
    private static final long serialVersionUID = -5528602631731589822L;
    private static Map nameToTypeMap = new HashMap();
    public Type(String name) {
        this.name = name;
        nameToTypeMap.put(name, this);
    }
    private String name;
    public String toString() { return name; }
    
    
    /*
     * Ssee http://www.javaworld.com/javaworld/javatips/jw-javatip122.html
     */
    private Object readResolve() {
        return nameToTypeMap.get(name);
    }
  }

  /**
   * Fixed Precision indicates that coordinates have a fixed number of decimal places.
   * The number of decimal places is determined by the log10 of the scale factor.
   */
  public static final Type FIXED = new Type("FIXED");
  /**
   * Floating precision corresponds to the standard Java
   * double-precision floating-point representation, which is
   * based on the IEEE-754 standard
   */
  public static final Type FLOATING = new Type("FLOATING");
  /**
   * Floating single precision corresponds to the standard Java
   * single-precision floating-point representation, which is
   * based on the IEEE-754 standard
   */
  public static final Type FLOATING_SINGLE = new Type("FLOATING SINGLE");


  /**
   *  The maximum precise value representable in a double. Since IEE754
   *  double-precision numbers allow 53 bits of mantissa, the value is equal to
   *  2^53 - 1.  This provides <i>almost</i> 16 decimal digits of precision.
   */
  public final static double maximumPreciseValue = 9007199254740992.0;

  /**
   * The type of PrecisionModel this represents.
   */
  private Type modelType;
  /**
   * The scale factor which determines the number of decimal places in fixed precision.
   */
  private double scale;

  /**
   * Creates a <code>PrecisionModel</code> with a default precision
   * of FLOATING.
   */
  public PrecisionModel() {
    // default is floating precision
    modelType = FLOATING;
  }

  /**
   * Creates a <code>PrecisionModel</code> that specifies
   * an explicit precision model type.
   * If the model type is FIXED the scale factor will default to 1.
   *
   * @param modelType the type of the precision model
   */
  public PrecisionModel(Type modelType)
  {
    this.modelType = modelType;
    if (modelType == FIXED)
    {
      setScale(1.0);
    }
  }
  /**
   *  Creates a <code>PrecisionModel</code> that specifies Fixed precision.
   *  Fixed-precision coordinates are represented as precise internal coordinates,
   *  which are rounded to the grid defined by the scale factor.
   *
   *@param  scale    amount by which to multiply a coordinate after subtracting
   *      the offset, to obtain a precise coordinate
   *@param  offsetX  not used.
   *@param  offsetY  not used.
   *
   * @deprecated offsets are no longer supported, since internal representation is rounded floating point
   */
  public PrecisionModel(double scale, double offsetX, double offsetY) {
    modelType = FIXED;
    setScale(scale);
  }
  /**
   *  Creates a <code>PrecisionModel</code> that specifies Fixed precision.
   *  Fixed-precision coordinates are represented as precise internal coordinates,
   *  which are rounded to the grid defined by the scale factor.
   *
   *@param  scale    amount by which to multiply a coordinate after subtracting
   *      the offset, to obtain a precise coordinate
   */
  public PrecisionModel(double scale) {
    modelType = FIXED;
    setScale(scale);
  }
  /**
   *  Copy constructor to create a new <code>PrecisionModel</code>
   *  from an existing one.
   */
  public PrecisionModel(PrecisionModel pm) {
    modelType = pm.modelType;
    scale = pm.scale;
  }


  /**
   * Tests whether the precision model supports floating point
   * @return <code>true</code> if the precision model supports floating point
   */
  public boolean isFloating()
  {
    return modelType == FLOATING || modelType == FLOATING_SINGLE;
  }

  /**
   * Returns the maximum number of significant digits provided by this
   * precision model.
   * Intended for use by routines which need to print out 
   * decimal representations of precise values (such as {@link WKTWriter}).
   * <p>
   * This method would be more correctly called
   * <tt>getMinimumDecimalPlaces</tt>, 
   * since it actually computes the number of decimal places
   * that is required to correctly display the full
   * precision of an ordinate value.
   * <p>
   * Since it is difficult to compute the required number of
   * decimal places for scale factors which are not powers of 10,
   * the algorithm uses a very rough approximation in this case.
   * This has the side effect that for scale factors which are
   * powers of 10 the value returned is 1 greater than the true value.
   * 
   *
   * @return the maximum number of decimal places provided by this precision model
   */
  public int getMaximumSignificantDigits() {
    int maxSigDigits = 16;
    if (modelType == FLOATING) {
      maxSigDigits = 16;
    } else if (modelType == FLOATING_SINGLE) {
      maxSigDigits = 6;
    } else if (modelType == FIXED) {
      maxSigDigits = 1 + (int) Math.ceil(Math.log(getScale()) / Math.log(10));
    }
    return maxSigDigits;
  }

  /**
   * Returns the scale factor used to specify a fixed precision model.
   * The number of decimal places of precision is 
   * equal to the base-10 logarithm of the scale factor.
   * Non-integral and negative scale factors are supported.
   * Negative scale factors indicate that the places 
   * of precision is to the left of the decimal point.  
   *
   *@return the scale factor for the fixed precision model
   */
  public double getScale() {
    return scale;
  }

  /**
   * Gets the type of this precision model
   * @return the type of this precision model
   * @see Type
   */
  public Type getType()
  {
    return modelType;
  }
  /**
   *  Sets the multiplying factor used to obtain a precise coordinate.
   * This method is private because PrecisionModel is an immutable (value) type.
   */
  private void setScale(double scale)
  {
    this.scale = Math.abs(scale);
  }

  /**
   * Returns the x-offset used to obtain a precise coordinate.
   *
   * @return the amount by which to subtract the x-coordinate before
   *         multiplying by the scale
   * @deprecated Offsets are no longer used
   */
  public double getOffsetX() {
    //We actually don't use offsetX and offsetY anymore ... [Jon Aquino]
    return 0;
  }



  /**
   * Returns the y-offset used to obtain a precise coordinate.
   *
   * @return the amount by which to subtract the y-coordinate before
   *         multiplying by the scale
   * @deprecated Offsets are no longer used
   */
  public double getOffsetY() {
    return 0;
  }

  /**
   *  Sets <code>internal</code> to the precise representation of <code>external</code>.
   *
   * @param external the original coordinate
   * @param internal the coordinate whose values will be changed to the
   *                 precise representation of <code>external</code>
   * @deprecated use makePrecise instead
   */
  public void toInternal (Coordinate external, Coordinate internal) {
    if (isFloating()) {
      internal.x = external.x;
      internal.y = external.y;
    }
    else {
      internal.x = makePrecise(external.x);
      internal.y = makePrecise(external.y);
    }
    internal.z = external.z;
  }

  /**
   *  Returns the precise representation of <code>external</code>.
   *
   *@param  external  the original coordinate
   *@return           the coordinate whose values will be changed to the precise
   *      representation of <code>external</code>
   * @deprecated use makePrecise instead
   */
  public Coordinate toInternal(Coordinate external) {
    Coordinate internal = new Coordinate(external);
    makePrecise(internal);
    return internal;
  }

  /**
   *  Returns the external representation of <code>internal</code>.
   *
   *@param  internal  the original coordinate
   *@return           the coordinate whose values will be changed to the
   *      external representation of <code>internal</code>
   * @deprecated no longer needed, since internal representation is same as external representation
   */
  public Coordinate toExternal(Coordinate internal) {
    Coordinate external = new Coordinate(internal);
    return external;
  }

  /**
   *  Sets <code>external</code> to the external representation of <code>internal</code>.
   *
   *@param  internal  the original coordinate
   *@param  external  the coordinate whose values will be changed to the
   *      external representation of <code>internal</code>
   * @deprecated no longer needed, since internal representation is same as external representation
   */
  public void toExternal(Coordinate internal, Coordinate external) {
      external.x = internal.x;
      external.y = internal.y;
  }

  /**
   * Rounds a numeric value to the PrecisionModel grid.
   * Asymmetric Arithmetic Rounding is used, to provide
   * uniform rounding behaviour no matter where the number is
   * on the number line.
   * <p>
   * This method has no effect on NaN values.
   * <p>
   * <b>Note:</b> Java's <code>Math#rint</code> uses the "Banker's Rounding" algorithm,
   * which is not suitable for precision operations elsewhere in JTS.
   */
  public double makePrecise(double val) 
  {
  	// don't change NaN values
  	if (Double.isNaN(val)) return val;
  	
  	if (modelType == FLOATING_SINGLE) {
  		float floatSingleVal = (float) val;
  		return (double) floatSingleVal;
  	}
  	if (modelType == FIXED) {
            return Math.round(val * scale) / scale;
//  		return Math.rint(val * scale) / scale;
  	}
  	// modelType == FLOATING - no rounding necessary
  	return val;
  }

  /**
   * Rounds a Coordinate to the PrecisionModel grid.
   */
  public void makePrecise(Coordinate coord)
  {
    // optimization for full precision
    if (modelType == FLOATING) return;

    coord.x = makePrecise(coord.x);
    coord.y = makePrecise(coord.y);
    //MD says it's OK that we're not makePrecise'ing the z [Jon Aquino]
  }


  public String toString() {
  	String description = "UNKNOWN";
  	if (modelType == FLOATING) {
  		description = "Floating";
  	} else if (modelType == FLOATING_SINGLE) {
  		description = "Floating-Single";
  	} else if (modelType == FIXED) {
  		description = "Fixed (Scale=" + getScale() + ")";
  	}
  	return description;
  }

  public boolean equals(Object other) {
    if (! (other instanceof PrecisionModel)) {
      return false;
    }
    PrecisionModel otherPrecisionModel = (PrecisionModel) other;
    return modelType == otherPrecisionModel.modelType
        && scale == otherPrecisionModel.scale;
  }
  /**
   *  Compares this {@link PrecisionModel} object with the specified object for order.
   * A PrecisionModel is greater than another if it provides greater precision.
   * The comparison is based on the value returned by the
   * {@link #getMaximumSignificantDigits} method.
   * This comparison is not strictly accurate when comparing floating precision models
   * to fixed models; however, it is correct when both models are either floating or fixed.
   *
   *@param  o  the <code>PrecisionModel</code> with which this <code>PrecisionModel</code>
   *      is being compared
   *@return    a negative integer, zero, or a positive integer as this <code>PrecisionModel</code>
   *      is less than, equal to, or greater than the specified <code>PrecisionModel</code>
   */
  public int compareTo(Object o) {
    PrecisionModel other = (PrecisionModel) o;

    int sigDigits = getMaximumSignificantDigits();
    int otherSigDigits = other.getMaximumSignificantDigits();
    return (new Integer(sigDigits)).compareTo(new Integer(otherSigDigits));
//    if (sigDigits > otherSigDigits)
//      return 1;
//    else if
//    if (modelType == FLOATING && other.modelType == FLOATING) return 0;
//    if (modelType == FLOATING && other.modelType != FLOATING) return 1;
//    if (modelType != FLOATING && other.modelType == FLOATING) return -1;
//    if (modelType == FIXED && other.modelType == FIXED) {
//      if (scale > other.scale)
//        return 1;
//      else if (scale < other.scale)
//        return -1;
//      else
//        return 0;
//    }
//    Assert.shouldNeverReachHere("Unknown Precision Model type encountered");
//    return 0;
  }
}


