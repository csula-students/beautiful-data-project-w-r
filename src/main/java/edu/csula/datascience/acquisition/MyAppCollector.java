package edu.csula.datascience.acquisition;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.File;
import java.util.Collection;


public class MyAppCollector{
    MongoClient mongoClient;
    MongoDatabase database;
    MongoCollection<Document> collection;
    public MyAppCollector() {
        // establish database connection to MongoDB
        mongoClient = new MongoClient();

        // select as testing database
        database = mongoClient.getDatabase("big-data");

        // select collection by name
        collection = database.getCollection("myProject");
    }
    
    public Collection<Business> mungee(Collection<File> src) {
    	Collection<Business> businesses = null;
        return businesses;
    }

  
}
