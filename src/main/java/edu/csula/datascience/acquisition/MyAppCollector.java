package edu.csula.datascience.acquisition;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import com.google.gson.Gson;

public class MyAppCollector {

	// Gson library for sending json to elastic search
	Gson gson = new Gson();
	boolean HeaderFlag = false;
	String filePath = Paths.get("src/main/resources/").toAbsolutePath().toString()+"\\";
	
	// Code taken from ElasticSearchExample used for reading big csv files	
	public void mungeeCrime04(File csv, String[] HeaderList,String fileName) {
		Map<String,String> CrimeIDs = new HashMap<String,String>();
		String dateFormat = "mm/dd/yyyy hh:mm";
		try {
			// after reading the csv file, we will use CSVParser to parse though
			CSVParser parser = CSVParser.parse(csv, Charset.defaultCharset(), CSVFormat.EXCEL.withHeader());
			// for each record, we will add to ArrayList of it's object

			parser.forEach(record -> {
				
				System.out.println("Checking record # "+record.getRecordNumber());
				
				// cleaning up dirty data which doesn't have date or zipCode
				// location
				if (!record.get(HeaderList[0]).isEmpty() && !record.get(HeaderList[1]).isEmpty()) {
					String CrimeDate = FilterDate(record.get(HeaderList[0]), dateFormat);
					String CDescription = record.get(HeaderList[2]);
					String CStreet = record.get(HeaderList[3]);
					String CCity = record.get(HeaderList[4]);
					String CID = record.get(HeaderList[8]).isEmpty() ? String.valueOf(record.getRecordNumber()) : record.get(HeaderList[8]);
					String CGeoLocation = correctGeoPointsCrime(record.get(HeaderList[5]), record.get(HeaderList[6]));

					// Add zip code if exists and if not get it from station Identifier
					String CZipCode;
					if (record.get(HeaderList[7]).isEmpty()) {
						CZipCode = FilterZipCode(record.get(HeaderList[1]));
					} else {
						CZipCode = record.get(HeaderList[7]);
					}
					// if data doesn't contain ZIP code or coordinates delete it
					if (CZipCode.isEmpty() && CGeoLocation.isEmpty()) {
						System.out.println("DELETED FOR MISSING INFO");

					} else {
						if (!CrimeIDs.containsKey(CID)) {
							CrimeIDs.put(CID, "");
							Crime crime = new Crime(CID, CrimeDate, CDescription, CStreet, CCity, CGeoLocation, CZipCode);
							if (HeaderFlag == false) {
								saveCrime(crime,filePath+fileName,HeaderFlag);
								HeaderFlag = true;
							}else{
								saveCrime(crime,filePath+fileName,HeaderFlag);
								System.out.println("RECORDED INFO AT LINE NUMBER ======> " + record.getRecordNumber());
							}
						} else {
							System.out.println("=====> Dublicate found <=====");
						}
							
					}
				}
			});
			CrimeIDs.clear();
			HeaderFlag = false;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// Code taken from example found at https://examples.javacodegeeks.com/core-java/apache/commons/csv-commons/writeread-csv-files-with-apache-commons-csv-example/
	public void mungeeCrime15(File csv, String[] HeaderList,String fileName) {

		Map<String,String> CrimeIDs = new HashMap<String,String>();
		
		String dateFormat = "mm/dd/yyyy hh:mm";
		
		FileReader fileReader = null;

		CSVParser csvFileParser = null;

		try {
			// initialize FileReader object
			fileReader = new FileReader(csv);

			// initialize CSVParser object
			csvFileParser = CSVParser.parse(csv, Charset.defaultCharset(), CSVFormat.EXCEL.withHeader());

			// Get a list of CSV file records
			List<CSVRecord> csvRecords = csvFileParser.getRecords();

			// Read the CSV file records starting from the second record to skip
			for (int i = 1; i < csvRecords.size(); i++) {
				System.out.println("Checking record # "+i);
				CSVRecord record = csvRecords.get(i);
				if (!record.get(HeaderList[0]).isEmpty() && !record.get(HeaderList[1]).isEmpty()) {
					String CrimeDate = FilterDate(record.get(HeaderList[0]), dateFormat);
					String CDescription = record.get(HeaderList[2]);
					String CStreet = record.get(HeaderList[3]);
					String CCity = record.get(HeaderList[4]);
					String CID = record.get(HeaderList[7]).isEmpty() ? String.valueOf(record.getRecordNumber()) : record.get(HeaderList[7]);
					String CGeoLocation = correctGeoPointsCrime(record.get(HeaderList[5]), record.get(HeaderList[6]));

					String CZipCode = FilterZipCode(record.get(HeaderList[1]));

					// if data doesn't contain ZIP code or coordinates delete it
					if (CZipCode.isEmpty() && CGeoLocation.isEmpty()) {
						System.out.println("DELETED FOR MISSING INFO");
					} else {
						if (!CrimeIDs.containsKey(CID)) {
							CrimeIDs.put(CID, "");
							Crime crime = new Crime(CID, CrimeDate, CDescription, CStreet, CCity, CGeoLocation, CZipCode);
							if (HeaderFlag == false) {
								saveCrime(crime,filePath+fileName,HeaderFlag);
								HeaderFlag = true;
							}else{
								saveCrime(crime,filePath+fileName,HeaderFlag);
								System.out.println("RECORDED INFO AT LINE NUMBER ======> " + record.getRecordNumber());
							}
						} else {
							System.out.println("=====> Dublicate found <=====");
						}
					}	
				}
				}
			CrimeIDs.clear();
			HeaderFlag = false;
		} catch (Exception e) {
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
	}

	// Code taken from ElasticSearchExample used for reading big csv files
	public void mungeeBusiness(File csv, String[] HeaderList,String fileName) {

		Map<String,String> BusinessIDs = new HashMap<String,String>();
		
		String dateFormat = "mm/dd/yyyy";
		String MinDate = "01/01/2004";
		try {
			// after reading the csv file, we will use CSVParser to parse through
			CSVParser parser = CSVParser.parse(csv, Charset.defaultCharset(), CSVFormat.EXCEL.withHeader());
			// for each record, we will add to ArrayList of it's object

			parser.forEach(record -> {
				
				System.out.println("Checking record # "+record.getRecordNumber());
				
				// cleaning up dirty data which doesn't have date or zipCode location
				if (!record.get(HeaderList[0]).isEmpty() && !record.get(HeaderList[4]).isEmpty()) {
					String BDate = FilterDate(record.get(HeaderList[0]), dateFormat);

					// Check if date after 01/01/2004
					if (CheckDateRange(BDate, MinDate)) {
						String BName = record.get(HeaderList[1]);
						String BStreet = record.get(HeaderList[2]);
						String BCity = record.get(HeaderList[3]);
						String BID = record.get(HeaderList[6]).isEmpty() ? String.valueOf(record.getRecordNumber()) : record.get(HeaderList[6]);
						String BGeoLocation = correctGeoPointsBusiness(record.get(HeaderList[5]));

						// Add zip code if exists
						String BZipCode = record.get(HeaderList[4]).length() >= 5 ? record.get(HeaderList[4]).substring(0, 5) : "";
						
						// if data doesn't contain ZIP code or coordinates delete it
						if (BZipCode.isEmpty() && BGeoLocation.isEmpty()) {
							System.out.println("DELETED FOR MISSING INFO");

						} else {
							if (!BusinessIDs.containsKey(BID)) {
								BusinessIDs.put(BID, "");
								Business business = new Business(BID, BName, BStreet, BCity, BZipCode, BGeoLocation, BDate);
								if (HeaderFlag == false) {
									saveBusiness(business,filePath+fileName,HeaderFlag);
									HeaderFlag = true;
								}else{
									saveBusiness(business,filePath+fileName,HeaderFlag);
									System.out.println("RECORDED INFO AT LINE NUMBER ======> " + record.getRecordNumber());
								}
							} else {
								System.out.println("=====> Dublicate found <=====");
							}
								
							}
						}
					}
			});
			BusinessIDs.clear();
			HeaderFlag = false;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// Code taken from ElasticSearchExample used for reading big csv files
	public void mungeeProperty(File csv, String[] HeaderList,String fileName) {

		Map<String,String> PropertyIDs = new HashMap<String,String>();
		
		String dateFormat = "yyyymmdd";
		String MinDate = "01/01/2004";
		try {

			// after reading the csv file, we will use CSVParser to parse
			// through
			CSVParser parser = CSVParser.parse(csv, Charset.defaultCharset(), CSVFormat.EXCEL.withHeader());
			// for each record, we will add to ArrayList of it's object
			
			parser.forEach(record -> {
				
				System.out.println("Checking record # "+record.getRecordNumber());
				
				String PropertyType = record.get(HeaderList[1]);
				// Check if property is commercial type
				if (PropertyType.equals("Commercial")) {

					// Cleaning up dirty data which doesn't have date or zipCode
					if (!record.get(HeaderList[0]).isEmpty() && !record.get(HeaderList[5]).isEmpty()) {
						String PDate = FilterDate(record.get(HeaderList[0]), dateFormat);

						// Check if date is after 01/01/2004
						if (CheckDateRange(PDate, MinDate)) {
							String PUnitsNo = record.get(HeaderList[4]);

							
								String PStreet = record.get(HeaderList[2]);
								String PCity = record.get(HeaderList[3]);
								String PGeoLocation = correctGeoPointsCrime(record.get(HeaderList[6]),record.get(HeaderList[7]));
								String PID = record.get(HeaderList[8]).isEmpty() ? String.valueOf(record.getRecordNumber()) : record.get(HeaderList[8]);
								// Add zip code if exists and if not get it from
								// station identifier
								String PZipCode = record.get(HeaderList[5]);

								// if data doesn't contain ZIP code or coordinates delete it
								if (PZipCode.isEmpty() && PGeoLocation.isEmpty()) {
									System.out.println("DELETED FOR MISSING INFO");

								} else {
									if (!PropertyIDs.containsKey(PID)) {
										PropertyIDs.put(PID,PID);
										Property property = new Property(PID,PDate, PStreet, PCity, PZipCode, PUnitsNo,PGeoLocation);
										if (HeaderFlag == false) {
											saveProperty(property,filePath+fileName,HeaderFlag);
											HeaderFlag = true;
										}else{
											saveProperty(property,filePath+fileName,HeaderFlag);
											System.out.println("RECORDED INFO AT LINE NUMBER ======> " + record.getRecordNumber());
										}
									} else {
										System.out.println("=====> Dublicate found <=====");
									}
									
								}
							
						}
					}
				}
			});
			PropertyIDs.clear();
			HeaderFlag = false;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// Extract zipcode from Crime file
	public static String FilterZipCode(String zipCode) {
		if (!zipCode.isEmpty() && zipCode.length() >= 4) {
			String OnlyZipCode = zipCode.substring(4);
			if (containsOnlyNumbers(OnlyZipCode)) {
				return OnlyZipCode;
			}
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
	public static String FilterDate(String dateString, String format) {

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

	// Get correct format of GeoLocation
	public static String correctGeoPointsBusiness(String GeoPoint) {
		if (!GeoPoint.isEmpty()) {
			String newPoints = GeoPoint.substring(1, GeoPoint.length() - 1);
			String[] temp = newPoints.split(" ");
			String finalString = String.format("%.2f", Double.parseDouble(temp[0].substring(0, temp[0].length() - 1)))
					+ "," + String.format("%.2f", Double.parseDouble(temp[1]));
			return finalString;
		} else {
			return "";
		}
	}

	public static String correctGeoPointsCrime(String lat, String log) {
		String newlat, newlog;
		if (lat.isEmpty()) {
			newlat = "";
		} else {
			newlat = String.format("%.2f", Double.parseDouble(lat));
		}
		if (log.isEmpty()) {
			newlog = "";
		} else {
			newlog = String.format("%.2f", Double.parseDouble(log));
		}
		if (lat.isEmpty() && log.isEmpty()) {
			return "";
		} else {
			return newlat + "," + newlog;
		}
	}
	
	public void saveCrime(Crime Crime, String fileName,boolean headerFlag) {
		//Delimiter used in CSV file
		String NEW_LINE_SEPARATOR = "\n";
		
		String[] HeaderList = { "CRIME_ID","CRIME_DATE", "ZIP_CODE", "CRIME_DESCRIPTION", "STREET", "CITY", "GEO_LOCATION"};
		
		FileWriter fileWriter = null;
		
		CSVPrinter csvFilePrinter = null;
		
		//Create the CSVFormat object with "\n" as a record delimiter
        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator(NEW_LINE_SEPARATOR);
        
		try {
			//initialize FileWriter object
			fileWriter = new FileWriter(fileName, true);
			
			
			//initialize CSVPrinter object 
	        csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);
	        
	        if (!headerFlag) {
	        	//Create CSV file header
		        csvFilePrinter.printRecord(HeaderList);
			}
		        
				//Write a new object list to the CSV file
		            List DataRecord = new ArrayList();
		            DataRecord.add(Crime.getCID());
		            DataRecord.add(Crime.getCDate());
		            DataRecord.add(Crime.getZipCode());
		            DataRecord.add(Crime.getDescription());
		            DataRecord.add(Crime.getStreet());
		            DataRecord.add(Crime.getCity());
		            DataRecord.add(Crime.getGeoLocation());
		            csvFilePrinter.printRecord(DataRecord);			
		} catch (Exception e) {
			System.out.println("Error in CsvFileWriter !!!");
			e.printStackTrace();
		} finally {
			try {
				fileWriter.flush();
				fileWriter.close();
				csvFilePrinter.close();
			} catch (IOException e) {
				System.out.println("Error while flushing/closing fileWriter/csvPrinter !!!");
                e.printStackTrace();
			}
		}
		
	}
	
	public void saveBusiness(Business Business, String fileName,boolean headerFlag) {
		//Delimiter used in CSV file
		String NEW_LINE_SEPARATOR = "\n";
		
		String[] HeaderList = { "BUSINESS_ID", "BUSINESS_DATE", "BUSINESS_NAME", "BUSINESS_ADDRESS", "CITY", "ZIP_CODE", "GEO_LOCATION" };
		
		FileWriter fileWriter = null;
		
		CSVPrinter csvFilePrinter = null;
		
		//Create the CSVFormat object with "\n" as a record delimiter
        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator(NEW_LINE_SEPARATOR);
        
		try {
			//initialize FileWriter object
			fileWriter = new FileWriter(fileName, true);
			
			//initialize CSVPrinter object 
	        csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);
	        
	        if (!headerFlag) {
	        	//Create CSV file header
		        csvFilePrinter.printRecord(HeaderList);
			}
				//Write a new object list to the CSV file
		            List DataRecord = new ArrayList();
		            DataRecord.add(Business.getBID());
		            DataRecord.add(Business.getStartdate());
		            DataRecord.add(Business.getBName());
		            DataRecord.add(Business.getAddress());
		            DataRecord.add(Business.getCity());
		            DataRecord.add(Business.getZipCode());
		            DataRecord.add(Business.getGeoLocation());
		            csvFilePrinter.printRecord(DataRecord);
			
		} catch (Exception e) {
			System.out.println("Error in CsvFileWriter !!!");
			e.printStackTrace();
		} finally {
			try {
				fileWriter.flush();
				fileWriter.close();
				csvFilePrinter.close();
			} catch (IOException e) {
				System.out.println("Error while flushing/closing fileWriter/csvPrinter !!!");
                e.printStackTrace();
			}
		}
		
	}
	
	public void saveProperty(Property Property, String fileName,boolean headerFlag) {
		//Delimiter used in CSV file
		String NEW_LINE_SEPARATOR = "\n";
		
		String[] HeaderList = { "PROPERTY_ID","PROPERTY_DATE", "ZIP_CODE", "UNITS_NO", "STREET", "CITY", "GEO_LOCATION"};
		
		FileWriter fileWriter = null;
		
		CSVPrinter csvFilePrinter = null;
		
		//Create the CSVFormat object with "\n" as a record delimiter
        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator(NEW_LINE_SEPARATOR);
        
		try {
			//initialize FileWriter object
			fileWriter = new FileWriter(fileName, true);
			
			
			//initialize CSVPrinter object 
	        csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);
	        
	        if (!headerFlag) {
	        	//Create CSV file header
		        csvFilePrinter.printRecord(HeaderList);
			}
		        
				//Write a new object list to the CSV file
		            List DataRecord = new ArrayList();
		            DataRecord.add(Property.getPID());
		            DataRecord.add(Property.getRecordingDate());
		            DataRecord.add(Property.getZipCode());
		            DataRecord.add(Property.getUnitNo());
		            DataRecord.add(Property.getStreetName());
		            DataRecord.add(Property.getCityName());
		            DataRecord.add(Property.getGeoLocation());
		            csvFilePrinter.printRecord(DataRecord);
			
		} catch (Exception e) {
			System.out.println("Error in CsvFileWriter !!!");
			e.printStackTrace();
		} finally {
			try {
				fileWriter.flush();
				fileWriter.close();
				csvFilePrinter.close();
			} catch (IOException e) {
				System.out.println("Error while flushing/closing fileWriter/csvPrinter !!!");
                e.printStackTrace();
			}
		}
		
	}

}
