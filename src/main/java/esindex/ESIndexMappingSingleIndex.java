package esindex;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.commons.lang.RandomStringUtils;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;

public class ESIndexMappingSingleIndex {
	public static void main(String[] args) throws SecurityException,
			IOException {
		/*
		 * Command Line arguments
		 */

		if (args.length != 8) {
			System.out
					.println("java -jar ESIndexMappingSingleIndex <ESHOST_NAME>"
							+ " <ES_PORTNUM> <ES_CLUSERNAME> <num_of_iterations> "
							+ "<index_name> <type_name> <logFileName> <number_of_documents> <num_of_fields> ");

			System.exit(0);
		}
		String esHostName = args[0];
		int esPortNum = Integer.parseInt(args[1]);
		String esClusterName = args[2];
		long numOfIterations = Long.parseLong(args[3]);
		String indexName = args[4];
		String typeName = args[5];
		String logFileName = args[6];
		int numOfDocuments = Integer.parseInt(args[7]);
		int numOfFields = Integer.parseInt(args[8]);

		String[] typeList = { "string", "int", "long", "float", "double",
				"boolean" };

		Random randomNum = new Random();

		Logger log = Logger
				.getLogger(ESIndexMappingSingleIndex.class.getName());
		FileHandler fh;
		fh = new FileHandler(logFileName);
		log.addHandler(fh);
		SimpleFormatter formatter = new SimpleFormatter();
		fh.setFormatter(formatter);

		/*
		 * ES node and client initialization.
		 */
		Node node = nodeBuilder().node();

		// Connects to Remote Client defined by the esHostName and Cluster
		// defined by esClusterName

		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", esClusterName).build();

		Client client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(esHostName,
						esPortNum));

		log.info("Starting Indexing.....");

		/* Define Mapping for the Index Before Indexing */
		Map<String, Object> mappingJson = new HashMap<String, Object>();
		for (int i = 1; i <= numOfFields; i++) {
			mappingJson.put("field" + i, typeList[randomNum.nextInt(6)]);
		}

		PutMappingResponse putMappingResponse = client.admin().indices()
				.preparePutMapping(indexName).setType(typeName)
				.setSource(mappingJson).execute().actionGet();

		long startTimeAllDocs = System.currentTimeMillis();
		long sumTimeIndivDocs = 0;

		for (int docId = 1; docId <= numOfDocuments; docId++) {
			long startTimeIndivDoc = System.currentTimeMillis();
			/* Populates the Map "jsonObject" for indexing */
			Map<String, Object> jsonObject = new HashMap<String, Object>();
			for (int i = 1; i <= numOfFields; i++) {
				jsonObject.put("field" + i,
						RandomStringUtils.randomAlphanumeric(5));
			}

			IndexResponse response = client
					.prepareIndex(indexName, typeName, String.valueOf(docId))
					.setSource(jsonObject).execute().actionGet();
			long endTimeIndivDoc = System.currentTimeMillis();
			long totalTimeIndivDoc = (endTimeIndivDoc - startTimeIndivDoc);
			sumTimeIndivDocs += totalTimeIndivDoc;
			log.info("Total Time (ms) to index document #" + docId + " : "
					+ totalTimeIndivDoc);

		}
		long endTimeAllDocs = System.currentTimeMillis();
		long totalTimeAllDocs = (endTimeAllDocs - startTimeAllDocs) / 1000;
		log.info("Total Time (s) to index all the documents [Sum Individual Docs]: "
				+ sumTimeIndivDocs);
		log.info("Total Time (s) to index all the documents [Outside Loop]: "
				+ totalTimeAllDocs);

	}

}
