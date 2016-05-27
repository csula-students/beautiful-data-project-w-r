package edu.csula.datascience.acquisition;

public class Property {

	private String RecordingDate;
	private String StreetName;
	private String CityName;
	private String ZipCode;
	private String UnitNo;
	private String GeoLocation;
	
	public Property(String recordingDate, String streetName, String cityName, String zipCode, String unitNo,
			String geoLocation) {
		
		this.RecordingDate = recordingDate;
		this.StreetName = streetName;
		this.CityName = cityName;
		this.ZipCode = zipCode;
		this.UnitNo = unitNo;
		this.GeoLocation = geoLocation;
	}

	public String getRecordingDate() {
		return RecordingDate;
	}

	public void setRecordingDate(String recordingDate) {
		RecordingDate = recordingDate;
	}

	public String getStreetName() {
		return StreetName;
	}

	public void setStreetName(String streetName) {
		StreetName = streetName;
	}

	public String getCityName() {
		return CityName;
	}

	public void setCityName(String cityName) {
		CityName = cityName;
	}

	public String getZipCode() {
		return ZipCode;
	}

	public void setZipCode(String zipCode) {
		ZipCode = zipCode;
	}

	public String getUnitNo() {
		return UnitNo;
	}

	public void setUnitNo(String unitNo) {
		UnitNo = unitNo;
	}

	public String getGeoLocation() {
		return GeoLocation;
	}

	public void setGeoLocation(String geoLocation) {
		GeoLocation = geoLocation;
	}
		
	
}
