package edu.csula.datascience.acquisition;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Map;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

public class MyAppCollectorApp {

	private final static String indexName = "bd-business";
	private final static String CrimeTypeName15 = "Crimes15";
	private final static String CrimeTypeName04 = "Crimes04";
	private final static String PropertyTypeName = "Properties";
	private final static String BusinessTypeName = "Businesses";
	private final static String localPath = Paths.get("src/main/resources/").toAbsolutePath().toString()+"\\";

	public static void main(String[] args) throws URISyntaxException {

		Node node = nodeBuilder()
				.settings(Settings.builder().put("cluster.name", "CS594").put("path.home", "elasticsearch-data"))
				.node();
		Client client = node.client();

		// create bulk processor
		BulkProcessor bulkProcessor = createBulkProcessor(client);		

		MyAppSource source = new MyAppSource();
		MyAppCollector collector = new MyAppCollector();

		String[] Crime15HeaderList = buildCrime15Header();
		String[] Crime04HeaderList = buildCrime04Header();
		String[] BusinessHeaderList = buildBusinessHeader();
		String[] PropertyHeaderList = buildPropertyHeader();

		// Download files from source
		source.Download("https://data.lacounty.gov/api/views/9trm-uz8i/rows.csv?accessType=DOWNLOAD&bom=true",
				"Property");
		source.Download("https://data.lacounty.gov/api/views/3dxh-c6jw/rows.csv?accessType=DOWNLOAD&bom=true",
				"Crime04-15");
		source.Download("https://data.lacounty.gov/api/views/ca5f-5zzs/rows.csv?accessType=DOWNLOAD&bom=true",
				"Crime2015");
		source.Download("https://data.lacity.org/api/views/r4uk-afju/rows.csv?accessType=DOWNLOAD&bom=true",
				"Businesses");

		// Get downloaded file
		File Crime15 = source.getDownloadedFile("Crime2015");
		System.out.println("CHECKING FILE: " + Crime15.getName());
		File Crime04 = source.getDownloadedFile("Crime04-15");
		System.out.println("CHECKING FILE: " + Crime04.getName());
		File Business = source.getDownloadedFile("Businesses");
		System.out.println("CHECKING FILE: " + Business.getName());
		File Property = source.getDownloadedFile("Property");
		System.out.println("CHECKING FILE: " + Property.getName());

		// Storing filtered data into CSV file
		//collector.mungeeCrime15(Crime15,Crime15HeaderList,"FilteredCrimes15.csv");
		
		//collector.mungeeCrime04(Crime04,Crime04HeaderList,"FilteredCrimes04.csv");
		
		//collector.mungeeBusiness(Business, BusinessHeaderList, "FilteredBusiness.csv");
		
		// TODO Need testing
		collector.mungeeProperty(Property, PropertyHeaderList, "FilteredProperty.csv");
		System.out.println("FINISHED SAVING");
		
		// TODO Insert filtered data into elastic search
		// TODO SHOW VISUALIZATION
	}

	public static void aggregation(Node node,String indexName,String typeName) {
		SearchResponse sr = node.client().prepareSearch(indexName)
		            .setTypes(typeName)
		            .setQuery(QueryBuilders.matchAllQuery())
		            .addAggregation(
		                AggregationBuilders.terms("stateAgg").field("state")
		                    .size(Integer.MAX_VALUE)
		            )
		            .execute().actionGet();

		        // Get your facet results
		        Terms agg1 = sr.getAggregations().get("stateAgg");

		        for (Terms.Bucket bucket: agg1.getBuckets()) {
		            System.out.println(bucket.getKey() + ": " + bucket.getDocCount());
		        }
	}

	public static BulkProcessor createBulkProcessor(Client client) {
		return BulkProcessor.builder(client, new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				System.out.println("Facing error while importing data to elastic search");
				failure.printStackTrace();
			}
		}).setBulkActions(10000).setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
				.setFlushInterval(TimeValue.timeValueSeconds(5)).setConcurrentRequests(1)
				.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)).build();
	}

	public static String[] buildCrime15Header() {
		String[] header = { "﻿CRIME_DATE", "STATION_IDENTIFIER", "CRIME_CATEGORY_DESCRIPTION", "STREET", "CITY",
				"LATITUDE", "LONGITUDE","CRIME_IDENTIFIER" };
		return header;
	}

	public static String[] buildCrime04Header() {
		String[] header = { "﻿CRIME_DATE", "STATION_IDENTIFIER", "CRIME_CATEGORY_DESCRIPTION", "STREET", "CITY",
				"LATITUDE", "LONGITUDE", "ZIP","CRIME_IDENTIFIER" };
		return header;
	}

	public static String[] buildBusinessHeader() {
		String[] header = { "LOCATION START DATE", "BUSINESS NAME", "STREET ADDRESS", "CITY", "ZIP CODE", "LOCATION","LOCATION ACCOUNT #" };
		return header;
	}

	public static String[] buildPropertyHeader() {
		String[] header = { "RecordingDate", "GeneralUseType", "StreetName", "City", "Units", "ZIPcode5", "CENTER_LAT",
				"CENTER_LON" , "AssessorID"};
		return header;
	}

}
