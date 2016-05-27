package edu.csula.datascience.acquisition;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.File;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

public class MyAppCollectorApp {
	
	 private final static String indexName = "bd-data";
	 private final static String typeName = "Crimes";
	
    public static void main(String[] args) throws URISyntaxException {
    	
    	Node node = nodeBuilder().settings(Settings.builder()
                .put("cluster.name", "CS594team")
                .put("path.home", "elasticsearch-data")).node();
            Client client = node.client();
        
    	MyAppSource source = new MyAppSource();
        MyAppCollector collector = new MyAppCollector();
        
        String[] Crime15HeaderList = buildCrime15Header();
        String[] Crime04HeaderList = buildCrime04Header();
        String[] BusinessHeaderList = buildBusinessHeader();
        String[] PropertyHeaderList = buildPropertyHeader();
        
        // Download files from source
        source.Download("https://data.lacounty.gov/api/views/9trm-uz8i/rows.csv?accessType=DOWNLOAD&bom=true","Property");
        source.Download("https://data.lacounty.gov/api/views/3dxh-c6jw/rows.csv?accessType=DOWNLOAD&bom=true", "Crime04-15");
        source.Download("https://data.lacounty.gov/api/views/ca5f-5zzs/rows.csv?accessType=DOWNLOAD&bom=true", "Crime2015");
        source.Download("https://data.lacity.org/api/views/r4uk-afju/rows.csv?accessType=DOWNLOAD&bom=true","Businesses");
        
  
        // Get downloaded file
        File Crime15 = source.getDownloadedFile("Crime2015");
        System.out.println("CHECKING FILE: " + Crime15.getName());
        File Crime04 = source.getDownloadedFile("Crime04-15");
        System.out.println("CHECKING FILE: " + Crime04.getName());
        File Business = source.getDownloadedFile("Businesses");
        System.out.println("CHECKING FILE: " + Business.getName());
        File Property = source.getDownloadedFile("Property");
        System.out.println("CHECKING FILE: " + Property.getName());
        
        
        // FINALL AND READY FOR USE
        //Map<String,Crime> crimes15 = collector.mungeeCrime15(Crime15, Crime15HeaderList);
        //System.out.println(crimes15.size());
        
        // FINALL AND READY FOR USE
        //Map<String,Crime> crimes04 = collector.mungeeCrime04(Crime04HeaderList,Crime04);
        //System.out.println(crimes04.size());
        
        // FINALL AND READY FOR USE
        //Map<String,Business> businesses = collector.mungeeBusiness(BusinessHeaderList,Business);
        //System.out.println(businesses.size());
        
        // FINALL AND READY FOR USE
        //Map<String,Property> properties = collector.mungeeProperty(PropertyHeaderList, Property);
        //System.out.println(properties.size());
        
        //TODO MAPS SHOULD BE of TYPE Map<String,ArrayList<Crime>>, Map<String,ArrayList<Business>> and Map<String,ArrayList<Property>>
        //TODO ADD TYPE FEILD TO THE DATA
        //TODO STORE FILTERED RESULTS IN ELASTICSEARCH
        //TODO SHOW VISUALIZATION
        
        // Not implemented
        //collector.save(cleanedTweets);
    }
    
    public static String[] buildCrime15Header(){
    	String[] header = {"﻿CRIME_DATE","STATION_IDENTIFIER","CRIME_CATEGORY_DESCRIPTION","STREET","CITY","LATITUDE","LONGITUDE"};
    	return header;
    }
    
    public static String[] buildCrime04Header(){
    	String[] header = {"﻿CRIME_DATE","STATION_IDENTIFIER","CRIME_CATEGORY_DESCRIPTION","STREET","CITY","LATITUDE","LONGITUDE","ZIP"};
    	return header;
    }
    
    public static String[] buildBusinessHeader(){
    	String[] header = {"LOCATION START DATE","BUSINESS NAME","STREET ADDRESS","CITY","ZIP CODE","LOCATION"};
    	return header;
    }
    
    public static String[] buildPropertyHeader(){
    	String[] header = {"RecordingDate","GeneralUseType","StreetName","City","Units","ZIPcode5","CENTER_LAT","CENTER_LON"};
    	return header;
    }
    
   
   
}
