package edu.csula.datascience.acquisition;

public class Business {

	private String BName;
	private String Address;
	private String City;
	private String zipCode;
	private String GeoLocation;
	private String Startdate;
	
	
	public Business(String bName, String address, String city, String zipCode, String geoLocation, String startdate) {
		this.BName = bName;
		this.Address = address;
		this.City = city;
		this.zipCode = zipCode;
		this.GeoLocation = geoLocation;
		this.Startdate = startdate;
	}
	
	public String getBName() {
		return BName;
	}
	public void setBName(String bName) {
		BName = bName;
	}
	public String getAddress() {
		return Address;
	}
	public void setAddress(String address) {
		Address = address;
	}
	public String getCity() {
		return City;
	}
	public void setCity(String city) {
		City = city;
	}
	public String getZipCode() {
		return zipCode;
	}
	public void setZipCode(String zipCode) {
		this.zipCode = zipCode;
	}
	public String getGeoLocation() {
		return GeoLocation;
	}
	public void setGeoLocation(String geoLocation) {
		GeoLocation = geoLocation;
	}
	public String getStartdate() {
		return Startdate;
	}
	public void setStartdate(String startdate) {
		Startdate = startdate;
	}
	
}
