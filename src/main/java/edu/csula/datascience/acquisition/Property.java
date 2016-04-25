package edu.csula.datascience.acquisition;

public class Property {

	private String cityName;
	private String zipCode;
	private String coordinates;
	private String establishDate;
	
	public Property(String cityName, String zipCode, String coordinates, String establishDate) {
		super();
		this.cityName = cityName;
		this.zipCode = zipCode;
		this.coordinates = coordinates;
		this.establishDate = establishDate;
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
	public String getEstablishDate() {
		return establishDate;
	}
	public void setEstablishDate(String establishDate) {
		this.establishDate = establishDate;
	}
	
}
