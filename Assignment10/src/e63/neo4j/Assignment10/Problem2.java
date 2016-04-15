package e63.neo4j.Assignment10;

import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;

// START n=node(*) MATCH (n)-[r]->(m) RETURN n,r,m;

/**
 * This java class is for Assignment 10 of e63 course (Big Data Analytics) at
 * Harvard Extension School for Spring 2016 Usage:
 * 
 * java e63.neo4j.Assignment10.Problem2 DB_SERVER_ROOT_URI DB_SERVER_USERNAME
 * DB_SERVER_PASSWORD
 * 
 * e.g. java e63.neo4j.Assignment10.Problem2 http://localhost:7474/db/data/
 * neo4j password
 * 
 * @author Rohan Pulekar
 *
 */
public class Problem2 {

	// neo4j database URI
	private static String DB_SERVER_ROOT_URI = "http://localhost:7474/db/data/";

	private static String DB_SERVER_USERNAME = "default_username";

	private static String DB_SERVER_PASSWORD = "default_password";

	private static Client webClient = null;

	/**
	 * The main method
	 * 
	 * 
	 * @param args
	 *            (not needed)
	 * @throws URISyntaxException
	 * @throws CustomException
	 */
	public static void main(String[] args) throws URISyntaxException {

		// check if database parameters are passed in as parameters
		if (args.length >= 1) {
			DB_SERVER_ROOT_URI = args[0];
		}

		if (args.length >= 2) {
			DB_SERVER_USERNAME = args[1];
		}

		if (args.length >= 3) {
			DB_SERVER_PASSWORD = args[2];
		}

		// create web client
		webClient = Client.create();

		// add basic http authentication filter
		webClient.addFilter(new HTTPBasicAuthFilter(DB_SERVER_USERNAME, DB_SERVER_PASSWORD));

		// check if neo4j database is available
		boolean isDatabaseAvailable = checkIfDatabaseIsAvailable();

		if (!isDatabaseAvailable) {
			System.out.println("Database not available.  Will exit");
			System.exit(1);
		}

		// create node of type Movie for The Matrix movie and add properties to
		// that node
		URI movieTheMatrixURI = createBlankNode();
		setLabelOnNode(movieTheMatrixURI, "Movie");
		addPropertyToNode(movieTheMatrixURI, "title", "The Matrix");
		addPropertyToNode(movieTheMatrixURI, "year", "1999-03-31");

		// create node of type Movie for The Matrix Reloaded movie and add
		// properties to that node
		URI movieTheMatrixReloadedURI = createBlankNode();
		setLabelOnNode(movieTheMatrixReloadedURI, "Movie");
		addPropertyToNode(movieTheMatrixReloadedURI, "title", "The Matrix Reloaded");
		addPropertyToNode(movieTheMatrixReloadedURI, "year", "2003-05-07");

		// create node of type Movie for The Matrix Revolutions and add
		// properties to that node
		URI movieTheMatrixRevolutionsURI = createBlankNode();
		setLabelOnNode(movieTheMatrixRevolutionsURI, "Movie");
		addPropertyToNode(movieTheMatrixRevolutionsURI, "title", "The Matrix Revolutions");
		addPropertyToNode(movieTheMatrixRevolutionsURI, "year", "2003-10-27");

		// create node of type Actor for William Dafoe and add properties to
		// that node
		URI actorWilliamDafoeNodeURI = createBlankNode();
		setLabelOnNode(actorWilliamDafoeNodeURI, "Actor");
		addPropertyToNode(actorWilliamDafoeNodeURI, "name", "William Dafoe");
		addPropertyToNode(actorWilliamDafoeNodeURI, "year_born", "1955");

		// create node of type Actor for Michael Nyquist and add properties to
		// that node
		URI actorMichaelNyquistURI = createBlankNode();
		setLabelOnNode(actorMichaelNyquistURI, "Actor");
		addPropertyToNode(actorMichaelNyquistURI, "name", "Michael Nyquist");
		addPropertyToNode(actorMichaelNyquistURI, "year_born", "1960");

		// create node of type Actor for Keanu Reeves and add properties to that
		// node
		URI actorKeanuReevesURI = createBlankNode();
		setLabelOnNode(actorKeanuReevesURI, "Actor");
		addPropertyToNode(actorKeanuReevesURI, "name", "Keanu Reeves");
		addPropertyToNode(actorKeanuReevesURI, "year_born", "1964");

		// create node of type Actor for Laurence Fishburne and add properties
		// to that node
		URI actorLaurenceFishburneURI = createBlankNode();
		setLabelOnNode(actorLaurenceFishburneURI, "Actor");
		addPropertyToNode(actorLaurenceFishburneURI, "name", "Laurence Fishburne");
		addPropertyToNode(actorLaurenceFishburneURI, "year_born", "1961");

		// create node of type Actor for Carrie-Ann Moss and add properties to
		// that node
		URI actorCarrieAnneMossURI = createBlankNode();
		setLabelOnNode(actorCarrieAnneMossURI, "Actor");
		addPropertyToNode(actorCarrieAnneMossURI, "name", "Carrie-Anne Moss");
		addPropertyToNode(actorCarrieAnneMossURI, "year_born", "1967");

		// create node of type Director for Chad Stahelski and add properties to
		// that node
		URI directorChadStahelskiURI = createBlankNode();
		setLabelOnNode(directorChadStahelskiURI, "Director");
		addPropertyToNode(directorChadStahelskiURI, "name", "Chad Stahelski");
		addPropertyToNode(directorChadStahelskiURI, "year_born", "1968");

		// create node of type Director for David Leitch and add properties to
		// that node
		URI directorDavidLeitchURI = createBlankNode();
		setLabelOnNode(directorDavidLeitchURI, "Director");
		addPropertyToNode(directorDavidLeitchURI, "name", "David Leitch");
		addPropertyToNode(directorDavidLeitchURI, "year_born", "1969");

		// create node of type Movie for John Wick and add properties to that
		// node
		URI movieJohnWickURI = createBlankNode();
		setLabelOnNode(movieJohnWickURI, "Movie");
		addPropertyToNode(movieJohnWickURI, "title", "John Wick");
		addPropertyToNode(movieJohnWickURI, "year", "2014-10-24");

		// create ACTS_IN relation between Keanu Reeves and The Matrix
		URI relation = createRelationshipBetweenNodes(actorKeanuReevesURI, movieTheMatrixURI, "ACTS_IN");
		addPropertyToRelationship(relation, "role", "Neo");

		// create ACTS_IN relation between Keanu Reeves and The Matrix Reloaded
		relation = createRelationshipBetweenNodes(actorKeanuReevesURI, movieTheMatrixReloadedURI, "ACTS_IN");
		addPropertyToRelationship(relation, "role", "Neo");

		// create ACTS_IN realtion between Keanu Reeves and The Matrix
		// Revolutions
		relation = createRelationshipBetweenNodes(actorKeanuReevesURI, movieTheMatrixRevolutionsURI, "ACTS_IN");
		addPropertyToRelationship(relation, "role", "Neo");

		// create ACTS_IN relation between Carrie Anne Moss and The Matrix
		relation = createRelationshipBetweenNodes(actorCarrieAnneMossURI, movieTheMatrixURI, "ACTS_IN");
		addPropertyToRelationship(relation, "role", "Trinity");

		// create ACTS_IN relation between Carrie Anne Moss and The Matrix
		// Reloaded
		relation = createRelationshipBetweenNodes(actorCarrieAnneMossURI, movieTheMatrixReloadedURI, "ACTS_IN");
		addPropertyToRelationship(relation, "role", "Trinity");

		// create ACTS_IN relation between Carrie Anne Moss and The Matrix
		// Revolutions
		relation = createRelationshipBetweenNodes(actorCarrieAnneMossURI, movieTheMatrixRevolutionsURI, "ACTS_IN");
		addPropertyToRelationship(relation, "role", "Trinity");

		// create ACTS_IN relation between Keanu Reeves and John Wick
		relation = createRelationshipBetweenNodes(actorKeanuReevesURI, movieJohnWickURI, "ACTS_IN");
		addPropertyToRelationship(relation, "role", "John Wick");

		// create ACTS_IN relation between William Dafoe and John Wick
		relation = createRelationshipBetweenNodes(actorWilliamDafoeNodeURI, movieJohnWickURI, "ACTS_IN");
		addPropertyToRelationship(relation, "role", "Marcus");

		// create ACTS_IN relation between Michael Nyquist and John Wick
		relation = createRelationshipBetweenNodes(actorMichaelNyquistURI, movieJohnWickURI, "ACTS_IN");
		addPropertyToRelationship(relation, "role", "Viggo Tarasov");

		// create DIRECTED relation between Chad Stahelski and John Wick
		relation = createRelationshipBetweenNodes(directorChadStahelskiURI, movieJohnWickURI, "DIRECTED");
		addPropertyToRelationship(relation, "shooting_camera_setup", "single camera");

		// create DIRECTED relation between David Leitch and John Wick
		relation = createRelationshipBetweenNodes(directorDavidLeitchURI, movieJohnWickURI, "DIRECTED");
		addPropertyToRelationship(relation, "shooting_camera_setup", "multi camera");

	}

	/**
	 * check if neo4j database is available
	 * 
	 * @return boolean (true|false)
	 */
	private static boolean checkIfDatabaseIsAvailable() {
		boolean databaseIsAvailable = false;

		// create webresource for the root url of neo4j database
		WebResource webResource = webClient.resource(DB_SERVER_ROOT_URI);

		// do a get on that web resource and get web response
		ClientResponse clientResponse = webResource.get(ClientResponse.class);

		// check response status
		if (clientResponse.getStatus() == 200) {
			System.out.format("Database connection successful");
			databaseIsAvailable = true;
		} else {
			System.out.format("Database is unavailable. Got status code: %d", clientResponse.getStatus());
		}

		return databaseIsAvailable;
	}

	/**
	 * THis method creates blank node
	 * 
	 * @return URI of the created node
	 * @throws CustomException
	 */
	private static URI createBlankNode() {

		// web URI for node
		final String nodeWebServiceURI = DB_SERVER_ROOT_URI + "node";

		// create web resource for node URI
		WebResource webResource = webClient.resource(nodeWebServiceURI);

		// make an empty web post to create a blank node
		ClientResponse clientResponse = webResource.post(ClientResponse.class);

		// check the web client response
		if (clientResponse.getStatus() != 201) {
			System.out.println("Unable to create actor node");
			System.exit(1);
		}

		// display message and close the client response
		System.out.println("Blank node created successfully");
		clientResponse.close();

		// return URI of the just created node
		return clientResponse.getLocation();
	}

	/**
	 * Sets label to the given node
	 * 
	 * @param nodeURI
	 * @param label
	 * @return
	 * @throws CustomException
	 */
	private static void setLabelOnNode(URI nodeURI, String label) {

		// create label web service end point URI for given node
		final String labelWebServiceURI = nodeURI.toString() + "/labels";

		// create web resource for label web service URI
		WebResource webResource = webClient.resource(labelWebServiceURI);

		// post label name to label web service end point
		ClientResponse clientResponse = webResource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, "[\"" + label + "\" ]");

		// check web service response status
		if (clientResponse.getStatus() != 204) {
			System.out.println("Unable to set label on node");
			System.exit(1);
		}

		// display status message and close the client response
		System.out.println("Label set on node");
		clientResponse.close();
	}

	/**
	 * add property to the given node
	 * 
	 * @param nodeUri
	 * @param propertyName
	 * @param propertyValue
	 */
	private static void addPropertyToNode(URI nodeUri, String propertyName, String propertyValue) {

		// create property web service URI for given node
		String propertyWebServiceURIForGivenNode = nodeUri.toString() + "/properties/" + propertyName;

		// create web resource for the property web service URI
		WebResource webResource = webClient.resource(propertyWebServiceURIForGivenNode);

		// use web service put method and put a string in property web service
		// URI. That string will be property value
		ClientResponse clientResponse = webResource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON)
				.entity("\"" + propertyValue + "\"").put(ClientResponse.class);

		// check the web service response
		if (clientResponse.getStatus() != 204) {
			System.out
					.println("Unable to add properties to the node. received errorcode:" + clientResponse.getStatus());
			System.exit(1);
		}

		// display success message and close the client response
		System.out.println("Successfully set the property on the node");

		// close the web service response
		clientResponse.close();
	}

	/**
	 * This method creates relationship between nodes
	 * 
	 * @param startNodeURI
	 * @param endNodeURI
	 * @param relationshipType
	 * @param jsonAttributes
	 * @return
	 * @throws URISyntaxException
	 */
	private static URI createRelationshipBetweenNodes(URI startNodeURI, URI endNodeURI, String relationshipType)
			throws URISyntaxException {

		// create URI for relationship web service end point of start node
		URI startNodeRelationshipURI = new URI(startNodeURI.toString() + "/relationships");

		// create json for relationship and end node
		String jsonForRelationship = createJsonForRelationship(endNodeURI, relationshipType);

		// create web resource for start node relationship URI
		WebResource webResource = webClient.resource(startNodeRelationshipURI);

		// POST json for relationship to the start node relationships URI
		ClientResponse clientResponse = webResource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON)
				.entity(jsonForRelationship).post(ClientResponse.class);

		// check status of web service response
		if (clientResponse.getStatus() != 201) {
			System.out.println("Unable to create relation. received errorcode:" + clientResponse.getStatus());
			System.exit(1);
		}

		// display success message
		System.out.println("Successfully created relation");

		// close the web service response
		clientResponse.close();

		// return relationship URI of the relationship just created
		return clientResponse.getLocation();
	}

	/**
	 * This method creates json for the given relationship for given to node.
	 * The json created is something like: { "to" :
	 * "http://localhost:7474/db/data/node/244", "type" : "ACTS_IN" }
	 * 
	 * @param endNodeURI
	 * @param relationshipType
	 * @return String (json representation)
	 */
	private static String createJsonForRelationship(URI endNodeURI, String relationshipType) {
		StringBuilder jsonStringBuilder = new StringBuilder();
		jsonStringBuilder.append("{ \"to\" : \"");
		jsonStringBuilder.append(endNodeURI.toString());
		jsonStringBuilder.append("\", ");

		jsonStringBuilder.append("\"type\" : \"");
		jsonStringBuilder.append(relationshipType);
		jsonStringBuilder.append("\"");

		jsonStringBuilder.append(" }");
		return jsonStringBuilder.toString();
	}

	/**
	 * Create property for relationship
	 * 
	 * @param relationshipURI
	 * @param propertyName
	 * @param propertyValue
	 * @throws URISyntaxException
	 */
	private static void addPropertyToRelationship(URI relationshipURI, String propertyName, String propertyValue)
			throws URISyntaxException {

		// create web service URI for properties of the relationship
		URI relationshipPropertyURI = new URI(relationshipURI.toString() + "/properties");

		// create json string representation of name and value
		String entity = createJSONForNameAndValue(propertyName, propertyValue);

		// create web resource from relationship property web service URI
		WebResource webResource = webClient.resource(relationshipPropertyURI);

		// put json string for name and value to web resource
		ClientResponse clientResponse = webResource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON)
				.entity(entity).put(ClientResponse.class);

		// check client response status
		if (clientResponse.getStatus() != 204) {
			System.out.println("Unable to add property to the relationship");
			System.exit(1);
		}
		System.out.println("Successfully added property to the relationship");

		// close the web service response
		clientResponse.close();
	}

	/**
	 * Create json string for name and value.
	 * 
	 * e.g. { "role" : "Neo" }
	 * 
	 * @param name
	 * @param value
	 * @return String (json representation of name and value)
	 */
	private static String createJSONForNameAndValue(String name, String value) {
		return String.format("{ \"%s\" : \"%s\" }", name, value);
	}

}
