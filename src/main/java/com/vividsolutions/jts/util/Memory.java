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
package com.vividsolutions.jts.util;

/**
 * Utility functions to report JVM memory usage.
 * 
 * @author mbdavis
 *
 */
public class Memory 
{
	public static long used()
	{
		Runtime runtime = Runtime.getRuntime ();
		return runtime.totalMemory() - runtime.freeMemory();
	}
	
	public static String usedString()
	{
		return format(used());
	}
	
	public static long free()
	{
		Runtime runtime = Runtime.getRuntime ();
		return runtime.freeMemory();
	}
	
	public static String freeString()
	{
		return format(free());
	}
	
	public static long total()
	{
		Runtime runtime = Runtime.getRuntime ();
		return runtime.totalMemory();
	}
	
	public static String totalString()
	{
		return format(total());
	}
	
	public static String usedTotalString()
	{
		return "Used: " + usedString() 
		+ "   Total: " + totalString();
	}
	
	public static String allString()
	{
		return "Used: " + usedString() 
		+ "   Free: " + freeString()
		+ "   Total: " + totalString();
	}
	
	public static final double KB = 1024;
	public static final double MB = 1048576;
	public static final double GB = 1073741824;

	public static String format(long mem)
	{
		if (mem < 2 * KB)
			return mem + " bytes";
		if (mem < 2 * MB)
			return round(mem / KB) + " KB";
		if (mem < 2 * GB)
			return round(mem / MB) + " MB";
		return round(mem / GB) + " GB";
	}
	
	public static double round(double d)
	{
		return Math.ceil(d * 100) / 100;
	}
}
