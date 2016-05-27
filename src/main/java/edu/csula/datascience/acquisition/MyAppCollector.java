package edu.csula.datascience.acquisition;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class MyAppCollector {	
	
	// Code taken from ElasticSearchExample used for reading big csv files
	public Map<String,Crime> mungeeCrime04(String[] HeaderList, File csv) {

		Map<String,Crime> crimes = new HashMap<String,Crime>();
		String dateFormat = "mm/dd/yyyy hh:mm";
		try {
			
			// after reading the csv file, we will use CSVParser to parse through
			CSVParser parser = CSVParser.parse(csv, Charset.defaultCharset(), CSVFormat.EXCEL.withHeader());
			// for each record, we will add to ArrayList of it's object
			
			parser.forEach(record -> {
				// cleaning up dirty data which doesn't have date or zipCode
				// location
				if (!record.get(HeaderList[0]).isEmpty() && !record.get(HeaderList[1]).isEmpty()) {
					String CrimeDate = FilterDate(record.get(HeaderList[0]),dateFormat);
					String CDescription = record.get(HeaderList[2]);
					String CStreet = record.get(HeaderList[3]);
					String CCity = record.get(HeaderList[4]);
					// TODO GeoLocation Need to convert to geoLocation object for elasticSearch
					String CGeoLocation = record.get(HeaderList[5]) +","+ record.get(HeaderList[6]);
					
					// Add zip code if exists and if not get it from station identifier
					String CZipCode;
					if(record.get(HeaderList[7]).isEmpty()){
						CZipCode = FilterZipCode(record.get(HeaderList[1]));
					}else{
						CZipCode = record.get(HeaderList[7]);
					}
					Crime crime = new Crime(CrimeDate, CDescription, CStreet, CCity, CGeoLocation, CZipCode);
					
					if (!crimes.containsKey(CrimeDate)) {
						crimes.put(CrimeDate, crime);
						System.out.println(crime.getCDate()+" | "+crime.getCity()+" | "+crime.getDescription()+" | "+crime.getGeoLocation()+" | "+crime.getStreet()+" | "+crime.getZipCode());
					}else{
						if (!crimes.get(CrimeDate).equals(crime)) {
							crimes.put(CrimeDate, crime);
							System.out.println(crime.getCDate()+" | "+crime.getCity()+" | "+crime.getDescription()+" | "+crime.getGeoLocation()+" | "+crime.getStreet()+" | "+crime.getZipCode());
						}
					}
				}				
			});
			return crimes;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	// Code taken from example found at https://examples.javacodegeeks.com/core-java/apache/commons/csv-commons/writeread-csv-files-with-apache-commons-csv-example/
		public Map<String,Crime> mungeeCrime15(File csv,String[] HeaderList) {

			String dateFormat = "mm/dd/yyyy hh:mm";
			FileReader fileReader = null;
			
			CSVParser csvFileParser = null;
			
	        try {
	        	
	        	//Create a new list of student to be filled by CSV file data 
	        	Map<String,Crime> crimes = new HashMap<String,Crime>();
	            
	            //initialize FileReader object
	            fileReader = new FileReader(csv);
	            
	            //initialize CSVParser object
	            csvFileParser = CSVParser.parse(csv, Charset.defaultCharset(), CSVFormat.EXCEL.withHeader());
	            
	            //Get a list of CSV file records
	            List<CSVRecord> csvRecords = csvFileParser.getRecords(); 
	            
	            //Read the CSV file records starting from the second record to skip the header
	            for (int i = 1; i < csvRecords.size(); i++) {
	            	CSVRecord record = csvRecords.get(i);
	            	if (!record.get(HeaderList[0]).isEmpty() && !record.get(HeaderList[1]).isEmpty()) {
						String CrimeDate = FilterDate(record.get(HeaderList[0]),dateFormat);
						String CDescription = record.get(HeaderList[2]);
						String CStreet = record.get(HeaderList[3]);
						String CCity = record.get(HeaderList[4]);
						// TODO GeoLocation Need to convert to geoLocation object for elasticSearch
						String CGeoLocation = record.get(HeaderList[5]) +","+ record.get(HeaderList[6]);
						String CZipCode = FilterZipCode(record.get(HeaderList[1]));

						Crime crime = new Crime(CrimeDate, CDescription, CStreet, CCity, CGeoLocation, CZipCode);
						if (!crimes.containsKey(CrimeDate)) {
							crimes.put(CrimeDate, crime);
						}else{
							if (!crimes.get(CrimeDate).equals(crime)) {
								crimes.put(CrimeDate, crime);
							}
						}
					}
				}
	            return crimes;
	        } 
	        catch (Exception e) {
	        	System.out.println("Error in CsvFileReader !!!");
	            e.printStackTrace();
	        } finally {
	            try {
	                fileReader.close();
	                csvFileParser.close();
	            } catch (IOException e) {
	            	System.out.println("Error while closing fileReader/csvFileParser !!!");
	                e.printStackTrace();
	            }
	       }
	        return null;
	     }

		// Code taken from ElasticSearchExample used for reading big csv files
		public Map<String,Business> mungeeBusiness(String[] HeaderList, File csv) {

			Map<String,Business> businesses = new HashMap<String,Business>();
			String dateFormat = "mm/dd/yyyy";
			try {
				
				// after reading the csv file, we will use CSVParser to parse through
				CSVParser parser = CSVParser.parse(csv, Charset.defaultCharset(), CSVFormat.EXCEL.withHeader());
				// for each record, we will add to ArrayList of it's object
				
				parser.forEach(record -> {
					// cleaning up dirty data which doesn't have date or zipCode
					// location
					if (!record.get(HeaderList[0]).isEmpty() && !record.get(HeaderList[4]).isEmpty()) {
						String BDate = FilterDate(record.get(HeaderList[0]),dateFormat);
						
						// Check if date after 01/01/2004
						if (CheckDateRange(BDate,"01/01/2004")) {
							String BName = record.get(HeaderList[1]);
							String BStreet = record.get(HeaderList[2]);
							String BCity = record.get(HeaderList[3]);
							// TODO GeoLocation Need to convert to geoLocation object for elasticSearch
							String BGeoLocation = record.get(HeaderList[5]);
							
							// Add zip code if exists
							String BZipCode = record.get(HeaderList[4]);
							
							Business business = new Business(BName, BStreet, BCity, BZipCode, BGeoLocation, BDate);
							
							if (!businesses.containsKey(BDate)) {
								businesses.put(BDate, business);
								System.out.println(business.getStartdate()+" | "+business.getBName()+" | "+business.getAddress()+" | "+business.getGeoLocation()+" | "+business.getCity()+" | "+business.getZipCode());
							}else{
								if (!businesses.get(BDate).equals(business)) {
									businesses.put(BDate, business);
									System.out.println(business.getStartdate()+" | "+business.getBName()+" | "+business.getAddress()+" | "+business.getGeoLocation()+" | "+business.getCity()+" | "+business.getZipCode());
									
								}
							}
						}
						
					}				
				});
				return businesses;
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
		
		// Code taken from ElasticSearchExample used for reading big csv files
		public Map<String,Business> mungeeProperty(String[] HeaderList, File csv) {

			Map<String,Business> businesses = new HashMap<String,Business>();
			String dateFormat = "mm/dd/yyyy";
			try {
				
				// after reading the csv file, we will use CSVParser to parse through
				CSVParser parser = CSVParser.parse(csv, Charset.defaultCharset(), CSVFormat.EXCEL.withHeader());
				// for each record, we will add to ArrayList of it's object
				
				parser.forEach(record -> {
					// cleaning up dirty data which doesn't have date or zipCode
					// location
					if (!record.get(HeaderList[0]).isEmpty() && !record.get(HeaderList[4]).isEmpty()) {
						String BDate = FilterDate(record.get(HeaderList[0]),dateFormat);
						String BName = record.get(HeaderList[1]);
						String BStreet = record.get(HeaderList[2]);
						String BCity = record.get(HeaderList[3]);
						// TODO GeoLocation Need to convert to geoLocation object for elasticSearch
						String BGeoLocation = record.get(HeaderList[5]);
						
						// Add zip code if exists and if not get it from station identifier
						String BZipCode = record.get(HeaderList[4]);
						
						Business business = new Business(BName, BStreet, BCity, BZipCode, BGeoLocation, BDate);
						
						if (!businesses.containsKey(BDate)) {
							businesses.put(BDate, business);
							System.out.println(business.getStartdate()+" | "+business.getBName()+" | "+business.getAddress()+" | "+business.getGeoLocation()+" | "+business.getCity()+" | "+business.getZipCode());
						}else{
							if (!businesses.get(BDate).equals(business)) {
								businesses.put(BDate, business);
								System.out.println(business.getStartdate()+" | "+business.getBName()+" | "+business.getAddress()+" | "+business.getGeoLocation()+" | "+business.getCity()+" | "+business.getZipCode());
								
							}
						}
					}				
				});
				return businesses;
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}

	// Extract zipcode from Crime file
	public static String FilterZipCode(String zipCode) {
		String OnlyZipCode = zipCode.substring(4);
		if (containsOnlyNumbers(OnlyZipCode)) {
			return OnlyZipCode;
		}
		return "";
	}

	// Helper class to check if String is contain only numbers
	public static boolean containsOnlyNumbers(String str) {
		for (int i = 0; i < str.length(); i++) {
			if (!Character.isDigit(str.charAt(i)))
				return false;
		}
		return true;
	}

	// Helper function that get correct date format
	public static String FilterDate(String dateString,String format) {

		SimpleDateFormat dt = new SimpleDateFormat(format);
		Date date;
		try {
			date = dt.parse(dateString);
			SimpleDateFormat dt1 = new SimpleDateFormat("mm/dd/yyyy");
			return dt1.format(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return dateString;
	}

	// Helper function to get only desired date
	public boolean CheckDateRange(String filteredDate, String MinDate) {
		SimpleDateFormat dt = new SimpleDateFormat("mm/dd/yyyy");
		Date date1, date2;

		try {
			date1 = dt.parse(filteredDate);
			date2 = dt.parse(MinDate);
			if (date1.after(date2)) {
				return true;
			} else {
				return false;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return false;
	}

	public void save(ArrayList<String> HeaderList, ArrayList<Object> MungedData, String fileName) {
	}

}
