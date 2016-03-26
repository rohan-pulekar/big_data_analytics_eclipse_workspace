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

@Deprecated
public class PreparedClientIncorrect {
	private Cluster cluster;
	private Session session;
	private BoundStatement boundStatementForSongs;
	private BoundStatement boundStatementForPlaylists;

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
		session.execute("CREATE TABLE prepared_client.songs (" + "id uuid PRIMARY KEY," + "title text," + "album text,"
				+ "artist text," + "tags set<text>," + "data blob" + ");");
		session.execute("CREATE TABLE prepared_client.playlists (" + "id uuid," + "title text," + "album text, "
				+ "artist text," + "song_id uuid," + "PRIMARY KEY (id, title, album, artist)" + ");");

	}

	public void createPreparedAndBoundStatments() {
		PreparedStatement statement = session
				.prepare("INSERT INTO prepared_client.songs (id, title, album, artist) " + "VALUES (?, ?, ?, ?);");
		boundStatementForSongs = new BoundStatement(statement);

		statement = session.prepare("INSERT INTO prepared_client.playlists (id, song_id, title, album, artist) "
				+ "VALUES (?, ?, ?, ?, ?);");
		boundStatementForPlaylists = new BoundStatement(statement);
	}

	public void loadData() {
		boundStatementForSongs.bind(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"), "La Petite Tonkinoise'",
				"Bye Bye Blackbird'", "Joséphine Baker");
		session.execute(boundStatementForSongs);

		boundStatementForPlaylists.bind(UUID.fromString("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
				UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"), "La Petite Tonkinoise", "Bye Bye Blackbird",
				"Josephine Baker");
		session.execute(boundStatementForPlaylists);
	}

	public void querySchema() {
		ResultSet results = session.execute(
				"SELECT * FROM prepared_client.playlists " + "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
		System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
				"-------------------------------+-----------------------+--------------------"));
		for (Row row : results) {
			System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"), row.getString("album"),
					row.getString("artist")));
		}
		System.out.println();

	}

	public void close() {
		cluster.close(); // .shutdown();
	}

	public static void main(String[] args) {
		PreparedClientIncorrect client = new PreparedClientIncorrect();
		client.connect("127.0.0.1");
		client.createSchema();
		client.createPreparedAndBoundStatments();
		client.loadData();
		client.querySchema();
		client.close();
	}
}