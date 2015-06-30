package com.marc.blazegraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openrdf.query.QueryLanguage;
import org.openrdf.query.Update;
import org.openrdf.repository.RepositoryConnection;

import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepository;

public class Main {

	private static final String REPO_URL = "http://localhost:9999/bigdata";
	private static final String NAMESPACE = "http://www.marc.com";
	private static final String CLASS_TYPE = "classes";
	private static final String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	private static final String RELATIONSHIP_TYPE = "Relationships#";

	private static final int REGION_COUNT = 4;
	private static final int COUNTRY_COUNT = 75;
	private static final int CITY_COUNT = 300;
	private static final int BUILDING_COUNT = 1000;
	private static final int FLOOR_COUNT = 2000;
	private static final int COMM_ROOM_COUNT = 5000;
	private static final int RACK_COUNT = 20000;

	private static BigdataSailRemoteRepository REPO;

	public static void main(String[] args) throws Exception {

		REPO = new BigdataSailRemoteRepository(REPO_URL);
		REPO.initialize();

		RepositoryConnection cxn = REPO.getConnection();
		generateGeneric(cxn, "Region", REGION_COUNT, false, "", 0);
		generateGeneric(cxn, "Country", COUNTRY_COUNT, true, "Region", REGION_COUNT);
		generateGeneric(cxn, "City", CITY_COUNT, true, "Country", COUNTRY_COUNT);
		generateGeneric(cxn, "Building", BUILDING_COUNT, true, "City", CITY_COUNT);
		generateGeneric(cxn, "Floor", FLOOR_COUNT, true, "Building", BUILDING_COUNT);
		generateGeneric(cxn, "CommRoom", COMM_ROOM_COUNT, true, "Floor", FLOOR_COUNT);
		generateGeneric(cxn, "Rack", RACK_COUNT, true, "CommRoom", COMM_ROOM_COUNT);
		cxn.commit();
		cxn.close();
		
		REPO.shutDown();
	}

	public static void generateGeneric(RepositoryConnection cxn, String classType, int classTypeCount,
			boolean hasChild, String childName, int childCount) throws Exception {

		List<String> queryList = new ArrayList<String>();

		for (int classId = 0; classId < classTypeCount; classId++) {
			String typeUriStr = "<" + NAMESPACE + "/" + CLASS_TYPE + "#" + classType + ">";
			String subjectUriStr = "<" + NAMESPACE + "/" + classType + "#" + classId + ">";

			queryList.add("INSERT DATA { " + subjectUriStr + " " + "<" + RDF_TYPE + ">" + " " + typeUriStr + " }; ");

			if (hasChild) {
				String locatedOnStr = "<" + NAMESPACE + "/" + RELATIONSHIP_TYPE + "/" + "Located_on" + ">";
				String childUriStr = "<" + NAMESPACE + "/" + childName + "#" + (classId % childCount) + ">";
				queryList.add("INSERT DATA { " + subjectUriStr + " " + locatedOnStr + " " + childUriStr + " }; ");
			}
		}

		List<List<String>> chunks = chunk(queryList, 50);
		for (List<String> chunk : chunks) {
			String query = "";
			for (String part : chunk) {
				query += part;
			}
			Update update = cxn.prepareUpdate(QueryLanguage.SPARQL, query);
			update.execute();
		}
	}

	public static <T> List<List<T>> chunk(List<T> input, int chunkSize) {

		int inputSize = input.size();
		int chunkCount = (int) Math.ceil(inputSize / (double) chunkSize);

		Map<Integer, List<T>> map = new HashMap<>(chunkCount);
		List<List<T>> chunks = new ArrayList<>(chunkCount);

		for (int i = 0; i < inputSize; i++) {

			map.computeIfAbsent(i / chunkSize, (ignore) -> {
				List<T> chunk = new ArrayList<>();
				chunks.add(chunk);
				return chunk;

			}).add(input.get(i));
		}
		return chunks;
	}
}
