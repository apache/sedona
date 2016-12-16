package org.datasyslab.geospark.enums;

public enum FileDataSplitter {
	CSV(","),
	TSV("\t"),
	GEOJSON(""),
	WKT("\t");
	
	public static FileDataSplitter getFileDataSplitter(String str) {
	    for (FileDataSplitter me : FileDataSplitter.values()) {
	        if (me.name().equalsIgnoreCase(str))
	            return me;
	    }
	    return null;
	}
	
	private String splitter;

	private FileDataSplitter(String splitter) {
		this.splitter = splitter;
	}
	
	public String getDelimiter() {
		return this.splitter;
	}
}

