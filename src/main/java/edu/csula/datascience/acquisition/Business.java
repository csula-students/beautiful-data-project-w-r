package edu.csula.datascience.acquisition;

public class Business {

	private String cityName;
	private String zipCode;
	private String Coordinates;
	private String Startdate;
	private String Enddate;
	
	public Business(String cityName, String zipCode, String coordinates, String startdate, String enddate) {
		this.cityName = cityName;
		this.zipCode = zipCode;
		Coordinates = coordinates;
		Startdate = startdate;
		Enddate = enddate;
	}
	
	public String getCityName() {
		return cityName;
	}
	public void setCityName(String cityName) {
		this.cityName = cityName;
	}
	public String getZipCode() {
		return zipCode;
	}
	public void setZipCode(String zipCode) {
		this.zipCode = zipCode;
	}
	public String getCoordinates() {
		return Coordinates;
	}
	public void setCoordinates(String coordinates) {
		Coordinates = coordinates;
	}
	public String getStartdate() {
		return Startdate;
	}
	public void setStartdate(String startdate) {
		Startdate = startdate;
	}
	public String getEnddate() {
		return Enddate;
	}
	public void setEnddate(String enddate) {
		Enddate = enddate;
	}
	
}
