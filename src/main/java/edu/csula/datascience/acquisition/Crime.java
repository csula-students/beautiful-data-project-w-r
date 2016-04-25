package edu.csula.datascience.acquisition;

public class Crime {
	
	private String cityName;
	private String zipCode;
	private String coordinates;
	private String date;
	
	public Crime(String cityName, String zipCode, String coordinates, String date) {
		this.cityName = cityName;
		this.zipCode = zipCode;
		this.coordinates = coordinates;
		this.date = date;
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
		return coordinates;
	}

	public void setCoordinates(String coordinates) {
		this.coordinates = coordinates;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

}
