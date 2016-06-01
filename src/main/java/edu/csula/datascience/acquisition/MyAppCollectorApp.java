package edu.csula.datascience.acquisition;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Paths;

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
	private final static String CrimeTypeName = "Crimes";
	private final static String PropertyTypeName = "Properties";
	private final static String BusinessTypeName = "Businesses";
	private final static String localPath = Paths.get("src/main/resources/").toAbsolutePath().toString() + "\\";

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

		// Get downloaded files
		File Crime04 = source.getDownloadedFile("Crime04-15");
		File Crime15 = source.getDownloadedFile("Crime2015");
		File Business = source.getDownloadedFile("Businesses");
		File Property = source.getDownloadedFile("Property");

		// Get filtered files
		File FilteredCrimes04 = new File(localPath + "FilteredCrimes04.csv");
		File FilteredCrimes15 = new File(localPath + "FilteredCrimes15.csv");
		File FilteredBusiness = new File(localPath + "FilteredBusiness.csv");
		File FilteredProperty = new File(localPath + "FilteredProperty.csv");

		// Mung & store data into csv files

		if (!FilteredCrimes04.exists()) {
			collector.mungeeCrime04(Crime04, Crime04HeaderList, "FilteredCrimes04.csv");
			System.out.println("FINISHED SAVING FilteredCrimes04.csv");
		} else {
			System.out.println("FILE " + FilteredCrimes04.getName() + " EXISTS");
		}

		if (!FilteredCrimes15.exists()) {
			collector.mungeeCrime15(Crime15, Crime15HeaderList, "FilteredCrimes15.csv");
			System.out.println("FINISHED SAVING FilteredCrimes15.csv");
		} else {
			System.out.println("FILE " + FilteredCrimes15.getName() + " EXISTS");
		}

		if (!FilteredBusiness.exists()) {
			collector.mungeeBusiness(Business, BusinessHeaderList, "FilteredBusiness.csv");
			System.out.println("FINISHED SAVING FilteredBusiness.csv");
		} else {
			System.out.println("FILE " + FilteredBusiness.getName() + " EXISTS");
		}

		if (!FilteredProperty.exists()) {
			collector.mungeeProperty(Property, PropertyHeaderList, "FilteredProperty.csv");
			System.out.println("FINISHED SAVING FilteredProperty.csv");
		} else {
			System.out.println("FILE " + FilteredProperty.getName() + " EXISTS");
		}
		
		collector.CombineCrimeFiles(FilteredCrimes15, FilteredCrimes04);
		System.out.println("Combining complete!");

		// Using FilteredCrime04 after combining Crime15 with it
		collector.Crime_ToElasticSearch(FilteredCrimes04,bulkProcessor,indexName,CrimeTypeName);
		collector.Business_ToElasticSearch(FilteredBusiness,bulkProcessor,indexName,BusinessTypeName);
		collector.Property_ToElasticSearch(FilteredProperty,bulkProcessor,indexName,PropertyTypeName);
		
		System.out.println("Uploading to elasticsearch is done!!");
		// TODO SHOW VISUALIZATION
		// TODO Add to AWS
	}

	public static void aggregation(Node node, String indexName, String typeName) {
		SearchResponse sr = node.client().prepareSearch(indexName).setTypes(typeName)
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(AggregationBuilders.terms("stateAgg").field("state").size(Integer.MAX_VALUE)).execute()
				.actionGet();

		// Get your facet results
		Terms agg1 = sr.getAggregations().get("stateAgg");

		for (Terms.Bucket bucket : agg1.getBuckets()) {
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
				"LATITUDE", "LONGITUDE", "CRIME_IDENTIFIER" };
		return header;
	}

	public static String[] buildCrime04Header() {
		String[] header = { "﻿CRIME_DATE", "STATION_IDENTIFIER", "CRIME_CATEGORY_DESCRIPTION", "STREET", "CITY",
				"LATITUDE", "LONGITUDE", "ZIP", "CRIME_IDENTIFIER" };
		return header;
	}

	public static String[] buildBusinessHeader() {
		String[] header = { "LOCATION START DATE", "BUSINESS NAME", "STREET ADDRESS", "CITY", "ZIP CODE", "LOCATION",
				"LOCATION ACCOUNT #" };
		return header;
	}

	public static String[] buildPropertyHeader() {
		String[] header = { "RecordingDate", "GeneralUseType", "StreetName", "City", "Units", "ZIPcode5", "CENTER_LAT",
				"CENTER_LON", "AssessorID" };
		return header;
	}

}
