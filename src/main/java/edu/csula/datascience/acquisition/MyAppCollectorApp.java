package edu.csula.datascience.acquisition;

import java.util.Collection;

public class MyAppCollectorApp {
    public static void main(String[] args) {
        MyAppSource source = new MyAppSource();
        MyAppCollector collector = new MyAppCollector();
        
        
        // Big file size 
        //source.Download("https://data.lacounty.gov/api/views/9trm-uz8i/rows.csv?accessType=DOWNLOAD&bom=true","Property");
        source.Download("https://data.lacounty.gov/api/views/3dxh-c6jw/rows.csv?accessType=DOWNLOAD&bom=true", "Crime04-15");
        source.Download("https://data.lacounty.gov/api/views/ca5f-5zzs/rows.csv?accessType=DOWNLOAD&bom=true", "Crime2015");
        source.Download("https://data.lacity.org/api/views/r4uk-afju/rows.csv?accessType=DOWNLOAD&bom=true","Businesses");
        
        
        /*
        Collection<Property> properties = source.getDownloadedFile("Property.csv");
        Collection<Crime> properties = source.getDownloadedFile("Crime04-15.csv");
        Collection<Crime> properties = source.getDownloadedFile("Crime2015.csv");
        Collection<Business> properties = source.getDownloadedFile("Businesses.csv");
        
        Collection<Business> cleanedProperties = collector.mungee(properties);*/
        
        // Not implemented
        //collector.save(cleanedTweets);
    }
}
