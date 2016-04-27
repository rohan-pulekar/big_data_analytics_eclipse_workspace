package e63.neo4j.Assignment10;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.core.MediaType;

import org.json.JSONArray;
import org.json.JSONObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;

/**
 * This java class is Problem2 and Problem3 for Assignment 10 of e63 course (Big
 * Data Analytics) at Harvard Extension School for Spring 2016 Usage:
 * 
 * java e63.neo4j.Assignment10.Problem2And3 DB_SERVER_ROOT_URI
 * DB_SERVER_USERNAME DB_SERVER_PASSWORD
 * 
 * e.g. java e63.neo4j.Assignment10.Problem2And3 http://localhost:7474/db/data/
 * neo4j password
 * 
 * @author Rohan Pulekar
 *
 */
public class Problem2And3 {

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

		// ********************* START OF PROBLEM 2 *********************
		System.out.println("********************* START OF PROBLEM 2 *********************");

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

		// create ACTS_IN relation between Laurence Fishburne and The Matrix
		relation = createRelationshipBetweenNodes(actorLaurenceFishburneURI, movieTheMatrixURI, "ACTS_IN");
		addPropertyToRelationship(relation, "role", "Morpheus");

		// create ACTS_IN relation between Laurence Fishburne and The Matrix
		// Reloaded
		relation = createRelationshipBetweenNodes(actorLaurenceFishburneURI, movieTheMatrixReloadedURI, "ACTS_IN");
		addPropertyToRelationship(relation, "role", "Morpheus");

		// create ACTS_IN relation between Laurence Fishburne and The Matrix
		// Revolutions
		relation = createRelationshipBetweenNodes(actorLaurenceFishburneURI, movieTheMatrixRevolutionsURI, "ACTS_IN");
		addPropertyToRelationship(relation, "role", "Morpheus");

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

		// ********************* END OF PROBLEM 2 *********************
		System.out.println("********************* END OF PROBLEM 2 *********************");

		System.out.println(System.lineSeparator());
		System.out.println(System.lineSeparator());
		// ********************* START OF PROBLEM 3 *********************
		System.out.println("********************* START OF PROBLEM 3 *********************");

		findOtherActorsInMoviesPlayedByKeanu(actorKeanuReevesURI);
		System.out.println(System.lineSeparator());
		System.out.println(System.lineSeparator());
		findDirectorsOfMoviesPlayedByKeanu(actorKeanuReevesURI);
		System.out.println(System.lineSeparator());

		// ********************* END OF PROBLEM 3 *********************
		System.out.println("********************* END OF PROBLEM 3 *********************");

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
	 * THis method creates blank node. This function is part of Problem2
	 * solution
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
	 * Sets label to the given node. This function is part of Problem2 solution
	 * 
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
	 * This function adds property to the given node. This function is part of
	 * Problem2 solution
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
	 * This function creates relationship between nodes. This function is part
	 * of Problem2 solution
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
	 * This function is part of Problem2 solution
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
	 * Create property for relationship. This function is part of Problem2
	 * solution
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
	 * This function is part of Problem2 solution
	 * 
	 * @param name
	 * @param value
	 * @return String (json representation of name and value)
	 */
	private static String createJSONForNameAndValue(String name, String value) {
		return String.format("{ \"%s\" : \"%s\" }", name, value);
	}

	/**
	 * This function finds movies acted in by an actor and then finds other
	 * actors who also acted in those movies actors who acted.
	 * 
	 * This function is part of Problem3 solution
	 * 
	 * @param keanuReevesNodeURI
	 * @throws URISyntaxException
	 */
	private static void findOtherActorsInMoviesPlayedByKeanu(URI keanuReevesNodeURI) throws URISyntaxException {

		// create traversal definition such that we can navigate movies in which
		// given actor has acted
		TraversalDefinition traversalDefinition = new TraversalDefinition();
		traversalDefinition.setOrder(TraversalDefinition.DEPTH_FIRST);
		traversalDefinition.setUniqueness(TraversalDefinition.NODE);
		traversalDefinition.setMaxDepth(10);
		traversalDefinition.setReturnFilter(TraversalDefinition.ALL);
		traversalDefinition.setRelationships(new Relation("ACTS_IN", Relation.OUT));

		// create web service URI for traversal from Keanu Reeves actor node
		URI actorNodeTraverserURI = new URI(keanuReevesNodeURI.toString() + "/traverse/node");

		// create resource for web service for traversal from actor node
		WebResource webResource = webClient.resource(actorNodeTraverserURI);

		// convert traverse definition to json string
		String jsonTraverserPayload = traversalDefinition.toJson();

		// make a post to get json response containing movies having ACTS_IN
		// relationship with this actor
		ClientResponse webResponse = webResource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON)
				.entity(jsonTraverserPayload).post(ClientResponse.class);

		// validate the web service response
		if (webResponse == null || webResponse.getStatus() != 200) {
			System.out.println("Something went wrong while searching for movies acted In By Keanu");
			System.exit(1);
		}

		// get the json response
		String jsonResponseAsString = webResponse.getEntity(String.class);

		// convert string json response to json obect
		JSONArray jsonArrayOfMovies = new JSONArray(jsonResponseAsString);

		if (jsonArrayOfMovies == null || jsonArrayOfMovies.length() == 0) {
			System.out.println("No movies found for the given actor");
		}

		// create variable for final collection of actors that have acted in
		// movies played by Keanu Reeves
		Set<String> finalListOfActors = new HashSet<String>();

		// loop through the movies played by Keanu Reeves
		for (Object object : jsonArrayOfMovies) {
			if (object == null || !(object instanceof JSONObject)) {
				continue;
			}
			JSONObject jsonObjectOfMovie = (JSONObject) object;

			// get the movie name and display it
			String movieName = String.valueOf(((JSONObject) jsonObjectOfMovie.get("data")).get("title").toString());
			System.out.println(String.format("Keanu Reeves acted in movie: %s", movieName));

			// get the movie node URI string and create URI object out of it
			String movieNodeURIString = String.valueOf(jsonObjectOfMovie.get("self"));
			URI movieNodeURI = new URI(movieNodeURIString);

			// call the below function to get actors who have acted in the movie
			Set<String> actorsInTheMovie = findActorsInAMovie(movieNodeURI);

			// remove Keanu Reeves onject from that list since we are lookign
			// for actors other than Keanu Reeves
			actorsInTheMovie.remove("Keanu Reeves");

			// display the actor names
			System.out.println(String.format("Other actors in that movie were: %s", actorsInTheMovie));
			System.out.print(System.lineSeparator());

			// add to the final list of actors, the actors list from current
			// movie
			finalListOfActors.addAll(actorsInTheMovie);
		}

		// display the full list of actors
		System.out.print(String.format("So full list of actors who acted in movies in which Keanu Reeves played: %s",
				finalListOfActors));

		// close the response
		webResponse.close();
	}

	/**
	 * This function returns a collection of actors who acted in the given
	 * movie.
	 * 
	 * This function is part of Problem3 solution
	 * 
	 * @param movieNodeURI
	 * @return Set<String> (collection of actors)
	 * @throws URISyntaxException
	 */
	private static Set<String> findActorsInAMovie(URI movieNodeURI) throws URISyntaxException {
		// create variable for final collection of actors that have acted in the
		// given movie
		Set<String> listOfActors = new HashSet<String>();

		// create traversal definition such that we can navigate actors who have
		// acted in the given movie
		TraversalDefinition traversalDefinition = new TraversalDefinition();
		traversalDefinition.setOrder(TraversalDefinition.DEPTH_FIRST);
		traversalDefinition.setUniqueness(TraversalDefinition.NODE);
		traversalDefinition.setMaxDepth(10);
		traversalDefinition.setReturnFilter(TraversalDefinition.ALL);
		traversalDefinition.setRelationships(new Relation("ACTS_IN", Relation.IN));

		// create web service URI for traversal from given movie node
		URI movieNodeTraverserURI = new URI(movieNodeURI.toString() + "/traverse/node");

		// create resource for web service for traversal from movie node
		WebResource webResource = webClient.resource(movieNodeTraverserURI);

		// convert traverse definition to json string
		String jsonTraverserPayload = traversalDefinition.toJson();

		// make a post to get json response containing actors having ACTS_IN
		// relationship with this movie
		ClientResponse webResponse = webResource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON)
				.entity(jsonTraverserPayload).post(ClientResponse.class);

		// validate the web service response
		if (webResponse == null || webResponse.getStatus() != 200) {
			System.out.println("Something went wrong while searching for movies acted In By actor");
			System.exit(1);
		}

		// get the json response
		String jsonResponseAsString = webResponse.getEntity(String.class);

		// convert string json response to json obect
		JSONArray jsonArrayOfActors = new JSONArray(jsonResponseAsString);

		if (jsonArrayOfActors == null || jsonArrayOfActors.length() == 0) {
			return listOfActors;
		}

		// loop through the movies played by Keanu Reeves
		for (Object object : jsonArrayOfActors) {
			if (object == null || !(object instanceof JSONObject)) {
				continue;
			}
			JSONObject jsonObjectOfActor = (JSONObject) object;

			// get the actor name
			String actorName = String.valueOf(((JSONObject) jsonObjectOfActor.get("data")).get("name"));

			// add the actor name to list of actors
			listOfActors.add(actorName);
		}

		// close the web service response
		webResponse.close();

		// return list of actors
		return listOfActors;
	}

	/**
	 * This function finds movies acted in by Keanu and then finds directors who
	 * directed those movies.
	 * 
	 * This function is part of Problem3 solution
	 * 
	 * @param keanuReevesNodeURI
	 * @throws URISyntaxException
	 */
	private static void findDirectorsOfMoviesPlayedByKeanu(URI keanuReevesNodeURI) throws URISyntaxException {

		// create traversal definition such that we can navigate movies in which
		// given actor has acted
		TraversalDefinition traversalDefinition = new TraversalDefinition();
		traversalDefinition.setOrder(TraversalDefinition.DEPTH_FIRST);
		traversalDefinition.setUniqueness(TraversalDefinition.NODE);
		traversalDefinition.setMaxDepth(10);
		traversalDefinition.setReturnFilter(TraversalDefinition.ALL);
		traversalDefinition.setRelationships(new Relation("ACTS_IN", Relation.OUT));

		// create web service URI for traversal from Keanu Reeves actor node
		URI actorNodeTraverserURI = new URI(keanuReevesNodeURI.toString() + "/traverse/node");

		// create resource for web service for traversal from actor node
		WebResource webResource = webClient.resource(actorNodeTraverserURI);

		// convert traverse definition to json string
		String jsonTraverserPayload = traversalDefinition.toJson();

		// make a post to get json response containing movies having ACTS_IN
		// relationship with this actor
		ClientResponse webResponse = webResource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON)
				.entity(jsonTraverserPayload).post(ClientResponse.class);

		// validate the web service response
		if (webResponse == null || webResponse.getStatus() != 200) {
			System.out.println("Something went wrong while searching for movies acted In By Keanu");
			System.exit(1);
		}

		// get the json response
		String jsonResponseAsString = webResponse.getEntity(String.class);

		// convert string json response to json obect
		JSONArray jsonArrayOfMovies = new JSONArray(jsonResponseAsString);

		if (jsonArrayOfMovies == null || jsonArrayOfMovies.length() == 0) {
			System.out.println("No movies found for the given actor");
		}

		// create variable for final collection of directors that have directed
		// in
		// movies played by Keanu Reeves
		Set<String> finalListOfDirectors = new HashSet<String>();

		// loop through the movies played by Keanu Reeves
		for (Object object : jsonArrayOfMovies) {
			if (object == null || !(object instanceof JSONObject)) {
				continue;
			}
			JSONObject jsonObjectOfMovie = (JSONObject) object;

			// get the movie name and display it
			String movieName = String.valueOf(((JSONObject) jsonObjectOfMovie.get("data")).get("title").toString());
			System.out.println(String.format("Keanu Reeves acted in movie: %s", movieName));

			// get the movie node URI string and create URI object out of it
			String movieNodeURIString = String.valueOf(jsonObjectOfMovie.get("self"));
			URI movieNodeURI = new URI(movieNodeURIString);

			// call the below function to get directors who have directed the
			// movie
			Set<String> directorsOfTheMovie = findDirectorsOfTheMovie(movieNodeURI);

			// display the director names
			if (!directorsOfTheMovie.isEmpty()) {
				System.out.println(String.format("Directors in that movie were: %s", directorsOfTheMovie));
			} else {
				System.out.println("No directors found for this movie");
			}
			System.out.print(System.lineSeparator());

			// add to the final list of directors, the directors list from
			// current
			// movie
			finalListOfDirectors.addAll(directorsOfTheMovie);
		}

		// display the full list of actors
		System.out.print(String.format("So full list of directors who directed movies in which Keanu Reeves played: %s",
				finalListOfDirectors));

		// close the response
		webResponse.close();
	}

	/**
	 * This function returns a collection of directors who directed the given
	 * movie.
	 * 
	 * This function is part of Problem3 solution
	 * 
	 * @param movieNodeURI
	 * @return Set<String> (collection of directors)
	 * @throws URISyntaxException
	 */
	private static Set<String> findDirectorsOfTheMovie(URI movieNodeURI) throws URISyntaxException {
		// create variable for final collection of directors that have directed
		// the
		// given movie
		Set<String> listOfDirectors = new HashSet<String>();

		// create traversal definition such that we can navigate directors who
		// have
		// directed in the given movie
		TraversalDefinition traversalDefinition = new TraversalDefinition();
		traversalDefinition.setOrder(TraversalDefinition.DEPTH_FIRST);
		traversalDefinition.setUniqueness(TraversalDefinition.NODE);
		traversalDefinition.setMaxDepth(10);
		traversalDefinition.setReturnFilter(TraversalDefinition.ALL);
		traversalDefinition.setRelationships(new Relation("DIRECTED", Relation.IN));

		// create web service URI for traversal from given movie node
		URI movieNodeTraverserURI = new URI(movieNodeURI.toString() + "/traverse/node");

		// create resource for web service for traversal from movie node
		WebResource webResource = webClient.resource(movieNodeTraverserURI);

		// convert traverse definition to json string
		String jsonTraverserPayload = traversalDefinition.toJson();

		// make a post to get json response containing directors having ACTS_IN
		// relationship with this movie
		ClientResponse webResponse = webResource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON)
				.entity(jsonTraverserPayload).post(ClientResponse.class);

		// validate the web service response
		if (webResponse == null || webResponse.getStatus() != 200) {
			System.out.println("Something went wrong while searching for movies acted In By director");
			System.exit(1);
		}

		// get the json response
		String jsonResponseAsString = webResponse.getEntity(String.class);

		// convert string json response to json obect
		JSONArray jsonArrayOfDirectors = new JSONArray(jsonResponseAsString);

		if (jsonArrayOfDirectors == null || jsonArrayOfDirectors.length() == 0) {
			return listOfDirectors;
		}

		// loop through the movies played by Keanu Reeves
		for (Object object : jsonArrayOfDirectors) {
			if (object == null || !(object instanceof JSONObject)) {
				continue;
			}
			JSONObject jsonObjectOfDirector = (JSONObject) object;

			// get the director name
			String directorName = String.valueOf(((JSONObject) jsonObjectOfDirector.get("data")).get("name"));

			// add the director name to list of directors
			listOfDirectors.add(directorName);
		}

		// close the web service response
		webResponse.close();

		// return list of directors
		return listOfDirectors;
	}

	private static class Relation {
		public static final String OUT = "out";
		public static final String IN = "in";
		private String type;
		private String direction;

		public String toJsonCollection() {
			StringBuilder sb = new StringBuilder();
			sb.append("{ ");
			sb.append(" \"type\" : \"" + type + "\"");
			if (direction != null) {
				sb.append(", \"direction\" : \"" + direction + "\"");
			}
			sb.append(" }");
			return sb.toString();
		}

		public Relation(String type, String direction) {
			setType(type);
			setDirection(direction);
		}

		public void setType(String type) {
			this.type = type;
		}

		public void setDirection(String direction) {
			this.direction = direction;
		}
	}

	private static class TraversalDefinition {
		public static final String DEPTH_FIRST = "depth first";
		public static final String NODE = "node";
		public static final String ALL = "all";

		private String uniqueness = NODE;
		private int maxDepth = 1;
		private String returnFilter = ALL;
		private String order = DEPTH_FIRST;
		private List<Relation> relationships = new ArrayList<Relation>();

		public void setOrder(String order) {
			this.order = order;
		}

		public void setUniqueness(String uniqueness) {
			this.uniqueness = uniqueness;
		}

		public void setMaxDepth(int maxDepth) {
			this.maxDepth = maxDepth;
		}

		public void setReturnFilter(String returnFilter) {
			this.returnFilter = returnFilter;
		}

		public void setRelationships(Relation... relationships) {
			this.relationships = Arrays.asList(relationships);
		}

		public String toJson() {
			StringBuilder sb = new StringBuilder();
			sb.append("{ ");
			sb.append(" \"order\" : \"" + order + "\"");
			sb.append(", ");
			sb.append(" \"uniqueness\" : \"" + uniqueness + "\"");
			sb.append(", ");
			if (relationships.size() > 0) {
				sb.append("\"relationships\" : [");
				for (int i = 0; i < relationships.size(); i++) {
					sb.append(relationships.get(i).toJsonCollection());
					if (i < relationships.size() - 1) { // Miss off the final
														// comma
						sb.append(", ");
					}
				}
				sb.append("], ");
			}
			sb.append("\"return filter\" : { ");
			sb.append("\"language\" : \"builtin\", ");
			sb.append("\"name\" : \"");
			sb.append(returnFilter);
			sb.append("\" }, ");
			sb.append("\"max depth\" : ");
			sb.append(maxDepth);
			sb.append(" }");
			return sb.toString();
		}
	}

}
