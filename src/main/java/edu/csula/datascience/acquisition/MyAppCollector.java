package edu.csula.datascience.acquisition;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;

import com.google.gson.Gson;

public class MyAppCollector {

	// Gson library for sending json to elastic search
	Gson gson = new Gson();

	// Code taken from ElasticSearchExample used for reading big csv files
	public Map<String, Crime> mungeeCrime04(File csv, String[] HeaderList, BulkProcessor bulkProcessor,
			String indexName, String typeName) {

		Map<String, Crime> crimes = new HashMap<String, Crime>();
		String dateFormat = "mm/dd/yyyy hh:mm";
		try {

			// after reading the csv file, we will use CSVParser to parse
			// through
			CSVParser parser = CSVParser.parse(csv, Charset.defaultCharset(), CSVFormat.EXCEL.withHeader());
			// for each record, we will add to ArrayList of it's object

			parser.forEach(record -> {
				// cleaning up dirty data which doesn't have date or zipCode
				// location
				if (!record.get(HeaderList[0]).isEmpty() && !record.get(HeaderList[1]).isEmpty()) {
					String CrimeDate = FilterDate(record.get(HeaderList[0]), dateFormat);
					String CDescription = record.get(HeaderList[2]);
					String CStreet = record.get(HeaderList[3]);
					String CCity = record.get(HeaderList[4]);
					String CID = record.get(HeaderList[8]).isEmpty() ? String.valueOf(record.getRecordNumber()) : record.get(HeaderList[8]);

					// TODO GeoLocation Need to testing after inserting into
					// elasticSearch
					String CGeoLocation = correctGeoPointsCrime(record.get(HeaderList[5]), record.get(HeaderList[6]));

					// Add zip code if exists and if not get it from station
					// identifier
					String CZipCode;
					if (record.get(HeaderList[7]).isEmpty()) {
						CZipCode = FilterZipCode(record.get(HeaderList[1]));
					} else {
						CZipCode = record.get(HeaderList[7]);
					}
					Crime crime = new Crime(CID, CrimeDate, CDescription, CStreet, CCity, CGeoLocation, CZipCode);

					// if data doesn't contain ZIP code or coordinates delete it
					if (CZipCode.isEmpty() && CGeoLocation.isEmpty()) {
						System.out.println("DELETED FOR MISSING INFO");

					} else {
						
						if (!crimes.containsKey(CID)) {
							crimes.put(CID, crime);
							System.out.println("DATA ADDED TO HAShMAP");
							// Adding to elastic search
							bulkProcessor.add(new IndexRequest(indexName, typeName).source(gson.toJson(crime)));
							System.out.println("DATA ADDED TO ElasticSearch");
							

						} else {
							
							System.out.println("DUBLICATE INFO current CID: "+CID+" Stored crime ID: "+crimes.get(CID).getCID());
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

	// Code taken from example found at
	// https://examples.javacodegeeks.com/core-java/apache/commons/csv-commons/writeread-csv-files-with-apache-commons-csv-example/
	public Map<String, Crime> mungeeCrime15(File csv, String[] HeaderList, BulkProcessor bulkProcessor,
			String indexName, String typeName) {

		String dateFormat = "mm/dd/yyyy hh:mm";
		FileReader fileReader = null;

		CSVParser csvFileParser = null;

		try {

			// Create a new list of student to be filled by CSV file data
			Map<String, Crime> crimes = new HashMap<String, Crime>();

			// initialize FileReader object
			fileReader = new FileReader(csv);

			// initialize CSVParser object
			csvFileParser = CSVParser.parse(csv, Charset.defaultCharset(), CSVFormat.EXCEL.withHeader());

			// Get a list of CSV file records
			List<CSVRecord> csvRecords = csvFileParser.getRecords();

			// Read the CSV file records starting from the second record to skip
			// the header
			for (int i = 1; i < csvRecords.size(); i++) {
				CSVRecord record = csvRecords.get(i);
				if (!record.get(HeaderList[0]).isEmpty() && !record.get(HeaderList[1]).isEmpty()) {
					String CrimeDate = FilterDate(record.get(HeaderList[0]), dateFormat);
					String CDescription = record.get(HeaderList[2]);
					String CStreet = record.get(HeaderList[3]);
					String CCity = record.get(HeaderList[4]);
					String CID = record.get(HeaderList[7]).isEmpty() ? String.valueOf(record.getRecordNumber()) : record.get(HeaderList[7]);

					// TODO GeoLocation Need to convert to geoLocation object
					// for elasticSearch
					String CGeoLocation = correctGeoPointsCrime(record.get(HeaderList[5]), record.get(HeaderList[6]));

					String CZipCode = FilterZipCode(record.get(HeaderList[1]));

					Crime crime = new Crime(CID, CrimeDate, CDescription, CStreet, CCity, CGeoLocation, CZipCode);

					// if data doesn't contain ZIP code or coordinates delete it
					if (CZipCode.isEmpty() && CGeoLocation.isEmpty()) {
						System.out.println("DELETED FOR MISSING INFO");

					} else {
						if (!crimes.containsKey(CID)) {
							crimes.put(CID, crime);
							System.out.println("DATA ADDED TO HAShMAP");
							// Adding to elastic search
							bulkProcessor.add(new IndexRequest(indexName, typeName).source(gson.toJson(crime)));
							System.out.println("DATA ADDED TO ElasticSearch");

						} else {
							System.out.println("DUBLICATE INFO current CID: "+CID+" Stored crime ID: "+crimes.get(CID).getCID());
						}
					}

				}
			}
			return crimes;
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
		return null;
	}

	// Code taken from ElasticSearchExample used for reading big csv files
	public Map<String, Business> mungeeBusiness(File csv, String[] HeaderList, BulkProcessor bulkProcessor,
			String indexName, String typeName) {

		Map<String, Business> businesses = new HashMap<String, Business>();
		String dateFormat = "mm/dd/yyyy";
		String MinDate = "01/01/2004";
		try {

			// after reading the csv file, we will use CSVParser to parse
			// through
			CSVParser parser = CSVParser.parse(csv, Charset.defaultCharset(), CSVFormat.EXCEL.withHeader());
			// for each record, we will add to ArrayList of it's object

			parser.forEach(record -> {
				// cleaning up dirty data which doesn't have date or zipCode
				// location
				if (!record.get(HeaderList[0]).isEmpty() && !record.get(HeaderList[4]).isEmpty()) {
					String BDate = FilterDate(record.get(HeaderList[0]), dateFormat);

					// Check if date after 01/01/2004
					if (CheckDateRange(BDate, MinDate)) {
						String BName = record.get(HeaderList[1]);
						String BStreet = record.get(HeaderList[2]);
						String BCity = record.get(HeaderList[3]);
						String BID = record.get(HeaderList[6]).isEmpty() ? String.valueOf(record.getRecordNumber()) : record.get(HeaderList[6]);
						// TODO GeoLocation Need to convert to geoLocation
						// object for elasticSearch
						String BGeoLocation = correctGeoPointsBusiness(record.get(HeaderList[5]));

						// Add zip code if exists
						String BZipCode = record.get(HeaderList[4]).length() >= 5
								? record.get(HeaderList[4]).substring(0, 5) : "";

						Business business = new Business(BID, BName, BStreet, BCity, BZipCode, BGeoLocation, BDate);

						// if data doesn't contain ZIP code or coordinates delete it
						if (BZipCode.isEmpty() && BGeoLocation.isEmpty()) {
							System.out.println("DELETED FOR MISSING INFO");

						} else {
							if (!businesses.containsKey(BID)) {
								businesses.put(BID, business);
								System.out.println("DATA ADDED TO HAShMAP");
								// Adding to elastic search
								bulkProcessor.add(new IndexRequest(indexName, typeName).source(gson.toJson(business)));
								System.out.println("DATA ADDED TO ElasticSearch");

							} else {
								System.out.println("DUBLICATE INFO current BID: "+BID+" Stored Business ID: "+businesses.get(BID).getBID());
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
	public Map<String, Property> mungeeProperty(File csv, String[] HeaderList, BulkProcessor bulkProcessor,
			String indexName, String typeName) {

		Map<String, Property> properties = new HashMap<String, Property>();
		String dateFormat = "yyyymmdd";
		String MinDate = "01/01/2004";
		try {

			// after reading the csv file, we will use CSVParser to parse
			// through
			CSVParser parser = CSVParser.parse(csv, Charset.defaultCharset(), CSVFormat.EXCEL.withHeader());
			// for each record, we will add to ArrayList of it's object

			parser.forEach(record -> {
				String PropertyType = record.get(HeaderList[1]);

				// Check if property is commercial type
				if (PropertyType.equals("Commercial")) {

					// Cleaning up dirty data which doesn't have date or zipCode
					if (!record.get(HeaderList[0]).isEmpty() && !record.get(HeaderList[5]).isEmpty()) {
						String PDate = FilterDate(record.get(HeaderList[0]), dateFormat);

						// Check if date is after 01/01/2004
						if (CheckDateRange(PDate, MinDate)) {
							String PUnitsNo = record.get(HeaderList[4]);

							// Check if unit counts are more than 0
							if (Integer.parseInt(PUnitsNo) > 0) {
								String PStreet = record.get(HeaderList[2]);
								String PCity = record.get(HeaderList[3]);
								// TODO GeoLocation Need to be tested when
								// inserted into elasticSearch
								String PGeoLocation = correctGeoPointsCrime(record.get(HeaderList[6]),
										record.get(HeaderList[7]));
								String PID = record.get(HeaderList[8]).isEmpty() ? String.valueOf(record.getRecordNumber()) : record.get(HeaderList[8]);
								// Add zip code if exists and if not get it from
								// station identifier
								String PZipCode = record.get(HeaderList[5]);

								Property property = new Property(PID,PDate, PStreet, PCity, PZipCode, PUnitsNo,
										PGeoLocation);

								// if data doesn't contain ZIP code or coordinates delete it
								if (PZipCode.isEmpty() && PGeoLocation.isEmpty()) {
									System.out.println("DELETED FOR MISSING INFO");

								} else {
									if (!properties.containsKey(PID)) {
										properties.put(PID, property);
										System.out.println("DATA ADDED TO HAShMAP");
										// Adding to elastic search
										bulkProcessor.add(new IndexRequest(indexName, typeName).source(gson.toJson(property)));
										System.out.println("DATA ADDED TO ElasticSearch");

									} else {
										System.out.println("DUBLICATE INFO current PID: "+PID+" Stored Properties ID: "+properties.get(PID).getPID());
									}
								}
							}

						}

					}
				}

			});
			return properties;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
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

	public void save(String[] HeaderList) {
	}

}
