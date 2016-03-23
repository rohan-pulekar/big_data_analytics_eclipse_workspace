package edu.hu.cassandra;

import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class PreparedClient {
	private Cluster cluster;
	private Session session;
	private BoundStatement boundStatementForSongs;

	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		session = cluster.connect("prepared_client");
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(),
					host.getRack());
		}
	}

	public void createSchema() {
		session.execute(
				"create table prepared_client.person (user_id uuid PRIMARY KEY, fname text, lname text, city text, cell_phone1 text, cell_phone2 text, cell_phone3 text);");
	}

	public void createPreparedAndBoundStatments() {
		PreparedStatement statement = session.prepare(
				"insert into prepared_client.person (user_id, fname, lname, city, cell_phone1, cell_phone2, cell_phone3) values (?, ?, ?, ?, ?, ?, ?);");
		boundStatementForSongs = new BoundStatement(statement);
	}

	public void loadData() {

		boundStatementForSongs.bind(UUID.randomUUID(), "Rohan", "Pulekar", "Waltham", "6177651008", "6176545244", null);
		session.execute(boundStatementForSongs);

		boundStatementForSongs.bind(UUID.randomUUID(), "Vinita", "Chaudhari", "Waltham", "8432759393", "6178555244",
				"9476665544");
		session.execute(boundStatementForSongs);

		boundStatementForSongs.bind(UUID.randomUUID(), "Gauri", "Pulekar", "Worcester", "3728484938", null, null);
		session.execute(boundStatementForSongs);
	}

	public void querySchema() {
		ResultSet results = session.execute("select * from prepared_client.person");
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
		PreparedClient client = new PreparedClient();

		System.out.println("\nOpening connection to Cassandra...");
		client.connect("127.0.0.1");
		System.out.println("...connected to Cassandra");

		System.out.println("\nCreating schema...");
		client.createSchema();
		System.out.println("...created schema");

		System.out.println("\nCreating prepared statements...");
		client.createPreparedAndBoundStatments();
		System.out.println("...created prepared statements");

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