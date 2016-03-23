package edu.hu.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CQLClientForProblem3 {
	private Cluster cluster;
	private Session session;

	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		session = cluster.connect("assignment7_problem3");
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(),
					host.getRack());
		}
	}

	public void createSchema() {
		session.execute(
				"create table assignment7_problem3.person (user_id uuid PRIMARY KEY, fname text, lname text, city text, cell_phone1 text, cell_phone2 text, cell_phone3 text);");
	}

	public void loadData() {
		session.execute(
				"insert into assignment7_problem3.person (user_id, fname, lname, city, cell_phone1, cell_phone2, cell_phone3) values (543216f7-2e54-4715-9f00-91dcbea6cf50, 'Rohan', 'Pulekar', 'Waltham', '6174591008', '6178555244', NULL);");
		session.execute(
				"insert into assignment7_problem3.person (user_id, fname, lname, city, cell_phone1, cell_phone2, cell_phone3) values (543216f7-2e54-4715-9f00-91dcbea6cf51, 'Vinita', 'Chaudhari', 'Waltham', '8432759393', '6178555244', '9476665544');");
		session.execute(
				"insert into assignment7_problem3.person (user_id, fname, lname, city, cell_phone1, cell_phone2, cell_phone3) values (543216f7-2e54-4715-9f00-91dcbea6cf52, 'Gauri', 'Pulekar', 'Worcester', '3728484938', NULL, NULL);");

	}

	public void querySchema() {
		ResultSet results = session.execute("select * from assignment7_problem3.person");
		System.out.println(String.format("%-40s%-20s%-20s%-20s%-20s%-20s%-20s", "user_id", "fname", "lname", "city",
				"cell_phone1", "cell_phone2", "cell_phone3"));
		System.out.println(
				"---------------------------------------+-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+");
		for (Row row : results) {
			System.out.println(String.format("%-40s%-20s%-20s%-20s%-20s%-20s%-20s", row.getUUID("user_id"),
					row.getString("fname"), row.getString("lname"), row.getString("city"), row.getString("cell_phone1"),
					row.getString("cell_phone2"), row.getString("cell_phone3")));
		}
	}

	public void close() {
		cluster.close(); // .shutdown();
	}

	public static void main(String[] args) {
		CQLClientForProblem3 client = new CQLClientForProblem3();

		System.out.println("\nOpening connection to Cassandra...");
		client.connect("127.0.0.1");
		System.out.println("...connected to Cassandra");

		System.out.println("\nCreating schema...");
		client.createSchema();
		System.out.println("...created schema");

		System.out.println("\nLoading data into the database...");
		client.loadData();
		System.out.println("...loaded data into the database");

		System.out.println("\nQuering the database...");
		client.querySchema();
		System.out.println("...queried the database");

		System.out.println("\nClosing connection to Cassandra...");
		client.close();
		System.out.println("...closed connection to Cassandra. All set!");
	}
}