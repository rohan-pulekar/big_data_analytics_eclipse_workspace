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

/**
 * This java class is for Assignment7 Problem4 of e63 course (Big Data
 * Analytics) of Harvard University
 * 
 * @author Rohan Pulekar
 *
 */
public class PreparedClient {
	private Cluster cluster;
	private Session session;
	private BoundStatement boundStatement;

	/**
	 * This function will connect to the Cassandra cluster specified by the URL
	 * in input parameter node. It will then create session object from that
	 * connection. It will then get metadata of that cluster. It will then get
	 * and print information of all hosts
	 * 
	 * @param node
	 *            (string containing the cluster URL)
	 */
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

	/**
	 * This function will create a table person in the Cassandra keyspace
	 * 'prepared_client'. It will use the session object created above.
	 */
	public void createSchema() {
		session.execute(
				"create table prepared_client.person (user_id uuid PRIMARY KEY, fname text, lname text, city text, cell_phone1 text, cell_phone2 text, cell_phone3 text);");
	}

	/**
	 * This function will create a prepared statement and a bound statement for
	 * table person. It will use the session object created above.
	 */
	public void createPreparedAndBoundStatments() {
		PreparedStatement preparedStatement = session.prepare(
				"insert into prepared_client.person (user_id, fname, lname, city, cell_phone1, cell_phone2, cell_phone3) values (?, ?, ?, ?, ?, ?, ?);");
		boundStatement = new BoundStatement(preparedStatement);
	}

	/**
	 * This function will insert data into the table person. It will use the
	 * boundStatement object created above.
	 */
	public void loadData() {
		// Here I have used UUID and used randomUUID() method of UUID class
		// to get a random UUID
		boundStatement.bind(UUID.randomUUID(), "Rohan", "Pulekar", "Waltham", "6177651008", "6176545244", null);
		session.execute(boundStatement);

		boundStatement.bind(UUID.randomUUID(), "Vinita", "Chaudhari", "Waltham", "8432759393", "6178555244",
				"9476665544");
		session.execute(boundStatement);

		boundStatement.bind(UUID.randomUUID(), "Gauri", "Pulekar", "Worcester", "3728484938", null, null);
		session.execute(boundStatement);
	}

	/**
	 * This function will query the person table and print out each row.
	 */
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

	/**
	 * This function will close the session and the cluster
	 */
	public void close() {
		session.close();
		cluster.close(); // .shutdown();
	}

	/**
	 * The main method
	 * 
	 * @param args
	 */
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