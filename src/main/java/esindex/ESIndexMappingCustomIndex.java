package esindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.lang.RandomStringUtils;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;

public class ESIndexMappingCustomIndex extends ConfigureClient {

	public static void main(String[] args) throws SecurityException,
			IOException {
		/*
		 * Command Line arguments
		 */

		if (args.length != 9) {
			System.out
					.println("java -jar ESIndexMappingCustomIndex <ESHOST_NAME>"
							+ " <ES_PORTNUM> <ES_CLUSERNAME> <indexNamePrefix> "
							+ "<type_name> <logFileName> <numOfIndexes> <number_of_documents> <num_of_fields> ");

			System.exit(0);
		}
		String esHostName = args[0];
		int esPortNum = Integer.parseInt(args[1]);
		String esClusterName = args[2];
		String indexNamePrefix = args[3];
		String typeName = args[4];
		String logFileName = args[5];
		int numOfIndexes = Integer.parseInt(args[6]);
		int numOfDocuments = Integer.parseInt(args[7]);
		int numOfFields = Integer.parseInt(args[8]);

		Logger log = setupLog(logFileName,
				ESIndexMappingCustomIndex.class.getName());

		/*
		 * ES node and client initialization.
		 */
		Client client = setupClient(esClusterName, esHostName, esPortNum);

		log.info("Starting Indexing.....");

		long startTimeAllIndex = System.currentTimeMillis();
		ArrayList<Long> indexTimeIndexes = new ArrayList<Long>();

		for (int indexId = 1; indexId <= numOfIndexes; indexId++) {
			long startTimeIndivIndex = System.currentTimeMillis();

			for (int docId = 1; docId <= numOfDocuments; docId++) {
				long startTimeIndivDoc = System.currentTimeMillis();
				/* Populates the Map "jsonObject" for indexing */
				Map<String, Object> jsonObject = new HashMap<String, Object>();
				for (int i = 1; i <= numOfFields; i++) {
					jsonObject.put(RandomStringUtils.randomAlphabetic(6),
							RandomStringUtils.randomAlphanumeric(5));
				}

				@SuppressWarnings("unused")
				IndexResponse response = client
						.prepareIndex(
								indexNamePrefix + String.valueOf(indexId),
								typeName, String.valueOf(docId))
						.setSource(jsonObject).execute().actionGet();
				long endTimeIndivDoc = System.currentTimeMillis();
				long totalTimeIndivDoc = (endTimeIndivDoc - startTimeIndivDoc);
				log.info("Total Indexing Time (ms) for index #" + indexId
						+ ", document #" + docId + " : " + totalTimeIndivDoc);
			}

			long endTimeIndivIndex = System.currentTimeMillis();
			long totaltimeIndivIndex = endTimeIndivIndex - startTimeIndivIndex;
			indexTimeIndexes.add(totaltimeIndivIndex);

		}
		long endTimeAllIndex = System.currentTimeMillis();
		long totalTimeAllIndex = (endTimeAllIndex - startTimeAllIndex);

		log.info("Total Time to index all the documents [Outside Loop]: "
				+ totalTimeAllIndex);

		/* Logging Index Level Time */

		log.info("### INDEX LEVEL ###");
		long finalSumIndexTime = 0;
		for (int i = 0; i < indexTimeIndexes.size(); i++) {
			log.info("Total Indexing Time (ms) for Index#" + (i + 1) + " is: "
					+ indexTimeIndexes.get(i));
			finalSumIndexTime += indexTimeIndexes.get(i);
		}

		log.info("Total time as Sum of Indexing all Indexes:  "
				+ finalSumIndexTime);
		// Closing Client
		closeClient(client);

	}
}
