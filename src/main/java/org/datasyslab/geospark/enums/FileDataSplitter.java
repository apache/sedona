package org.datasyslab.geospark.enums;

public enum FileDataSplitter {
	CSV(","),
	TSV("\t"),
	GEOJSON(""),
	WKT("\t");
	
	private String splitter;

	FileDataSplitter(String splitter) {
		this.splitter = splitter;
	}
	
	public String getSplitter() {
		return this.splitter;
	}
}

