package esindex;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.commons.lang.RandomStringUtils;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

public class ESIndexMappingSingleIndex extends ConfigureClient {
	public static void main(String[] args) throws SecurityException,
			IOException {
		/*
		 * Command Line arguments
		 */

		if (args.length != 8) {
			System.out
					.println("java -jar ESIndexMappingSingleIndex <ESHOST_NAME>"
							+ " <ES_PORTNUM> <ES_CLUSERNAME>  "
							+ "<index_name> <type_name> <logFileName> <number_of_documents> <num_of_fields> ");

			System.exit(0);
		}
		String esHostName = args[0];
		int esPortNum = Integer.parseInt(args[1]);
		String esClusterName = args[2];
		String indexName = args[3];
		String typeName = args[4];
		String logFileName = args[5];
		int numOfDocuments = Integer.parseInt(args[6]);
		int numOfFields = Integer.parseInt(args[7]);

		String[] typeList = { "string", "int", "long", "float", "double",
				"boolean" };

		Random randomNum = new Random();

		Logger log = setupLog(logFileName,
				ESIndexMappingSingleIndex.class.getName());

		Node node = nodeBuilder().node();
		Client client = setupClient(esClusterName, esHostName, esPortNum);

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

		// Closing Client
		closeClient(client);

	}

}
