package edu.csula.datascience.acquisition;

public class Property {

	private String CityName;
	private String ZipCode;
	private String HouseNo;
	private String StreetName;
	private String EffectiveDate;
	private String GeoLocation;
	
	public Property(String cityName, String zipCode, String houseNo, String streetName, String effectiveDate,
			String geoLocation) {
		this.CityName = cityName;
		this.ZipCode = zipCode;
		this.HouseNo = houseNo;
		this.StreetName = streetName;
		this.EffectiveDate = effectiveDate;
		this.GeoLocation = geoLocation;
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

	public String getHouseNo() {
		return HouseNo;
	}

	public void setHouseNo(String houseNo) {
		HouseNo = houseNo;
	}

	public String getStreetName() {
		return StreetName;
	}

	public void setStreetName(String streetName) {
		StreetName = streetName;
	}

	public String getEffectiveDate() {
		return EffectiveDate;
	}

	public void setEffectiveDate(String effectiveDate) {
		EffectiveDate = effectiveDate;
	}

	public String getGeoLocation() {
		return GeoLocation;
	}

	public void setGeoLocation(String geoLocation) {
		GeoLocation = geoLocation;
	}
	
	
	
	
}
