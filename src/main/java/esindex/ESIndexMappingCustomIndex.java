package esindex;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.commons.lang.RandomStringUtils;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;

public class ESIndexMappingCustomIndex {

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

		Logger log = Logger
				.getLogger(ESIndexMappingCustomIndex.class.getName());
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

		long startTimeAllDocs = System.currentTimeMillis();
		long sumTimeIndivDocs = 0;
		for (int indexId = 1; indexId <= numOfIndexes; indexId++) {
			for (int docId = 1; docId <= numOfDocuments; docId++) {
				long startTimeIndivDoc = System.currentTimeMillis();
				/* Populates the Map "jsonObject" for indexing */
				Map<String, Object> jsonObject = new HashMap<String, Object>();
				for (int i = 1; i <= numOfFields; i++) {
					jsonObject.put("field" + i,
							RandomStringUtils.randomAlphanumeric(5));
				}

				IndexResponse response = client
						.prepareIndex(
								indexNamePrefix + String.valueOf(indexId),
								typeName, String.valueOf(docId))
						.setSource(jsonObject).execute().actionGet();
				long endTimeIndivDoc = System.currentTimeMillis();
				long totalTimeIndivDoc = (endTimeIndivDoc - startTimeIndivDoc);
				sumTimeIndivDocs += totalTimeIndivDoc;
				log.info("Total Time (ms) to index document #" + docId + " : "
						+ totalTimeIndivDoc);

			}
		}
		long endTimeAllDocs = System.currentTimeMillis();
		long totalTimeAllDocs = (endTimeAllDocs - startTimeAllDocs) / 1000;
		log.info("Total Time (s) to index all the documents [Sum Individual Docs]: "
				+ sumTimeIndivDocs / 1000);
		log.info("Total Time (s) to index all the documents [Outside Loop]: "
				+ totalTimeAllDocs);

		// Closing
		client.close();

	}
}
