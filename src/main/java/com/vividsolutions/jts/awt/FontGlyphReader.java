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

package com.vividsolutions.jts.awt;

import java.util.*;
import java.awt.Font;
import java.awt.font.*;
import com.vividsolutions.jts.geom.*;

/**
 * Provides methods to read {@link Font} glyphs for strings 
 * into {@link Polygonal} geometry.
 * <p>
 * It is suggested to use larger point sizes to render fonts glyphs,
 * to reduce the effects of scale-dependent hints.
 * The result geometry is in the base coordinate system of the font.  
 * The geometry can be further transformed as necessary using
 * {@link AffineTransformation}s.
 * 
 * @author Martin Davis
 *
 */
public class FontGlyphReader 
{
  /**
   * The font name of the Java logical font Serif.
   */
  public static final String FONT_SERIF = "Serif";
  
  /**
   * The font name of the Java logical font SansSerif.
   * <p>
   * DEPRECATED - use FONT_SANSSERIF
   */
  public static final String FONT_SANSERIF = "SansSerif";
  
  
  /**
   * The font name of the Java logical font SansSerif.
   */
  public static final String FONT_SANSSERIF = "SansSerif";
  
  /**
   * The font name of the Java logical font Monospaced.
   */

  public static final String FONT_MONOSPACED = "Monospaced";
	
  // a flatness factor empirically determined to provide good results
  private static final double FLATNESS_FACTOR = 400;
  
  /**
   * Converts text rendered in the given font and pointsize to a {@link Geometry}
   * using a standard flatness factor.
   *  
   * @param text the text to render
   * @param fontName the name of the font
   * @param pointSize the pointSize to render at
   * @param geomFact the geometryFactory to use to create the result
   * @return a polygonal geometry representing the rendered text
   */
  public static Geometry read(String text, String fontName, int pointSize, GeometryFactory geomFact)
  {
    return read(text, new Font(fontName, Font.PLAIN, pointSize), geomFact);
  }
  
  /**
   * Converts text rendered in the given {@link Font} to a {@link Geometry}
   * using a standard flatness factor.
   * 
   * @param text the text to render
   * @param font  the font to render with
   * @param geomFact the geometryFactory to use to create the result
   * @return a polygonal geometry representing the rendered text
   */
  public static Geometry read(String text, Font font, GeometryFactory geomFact)
  {
    double flatness = font.getSize() / FLATNESS_FACTOR;
    return read(text, font, flatness, geomFact);
  }
  
  /**
   * Converts text rendered in the given {@link Font} to a {@link Geometry}
   * 
   * @param text the text to render
   * @param font  the font to render with
   * @param flatness the flatness factor to use
   * @param geomFact the geometryFactory to use to create the result
   * @return a polygonal geometry representing the rendered text
   */
  public static Geometry read(String text, Font font, double flatness, GeometryFactory geomFact)
  {
    char[] chs = text.toCharArray();
    FontRenderContext fontContext = new FontRenderContext(null, false, true);
    GlyphVector gv = font.createGlyphVector(fontContext, chs);
    List polys = new ArrayList();
    for (int i = 0; i < gv.getNumGlyphs(); i++) {
      Geometry geom = ShapeReader.read(gv.getGlyphOutline(i), flatness, geomFact);
      for (int j = 0; j < geom.getNumGeometries(); j++) {
        polys.add(geom.getGeometryN(j));
      }
    }
    return geomFact.buildGeometry(polys);
  }
      
}
