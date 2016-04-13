package e63.neo4j.Assignment10;

import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;

public class Problem2 {

	private static final String DB_SERVER_ROOT_URI = "http://localhost:7474/db/data/";

	public static void main(String[] args) throws URISyntaxException, CustomException {
		checkIfDatabaseIsAvailable();

		URI movieTheMatrixURI = createBlankNode();
		setLabelOnNode(movieTheMatrixURI, "Movie");
		addProperty(movieTheMatrixURI, "title", "The Matrix");
		addProperty(movieTheMatrixURI, "year", "1999-03-31");

		URI movieTheMatrixReloadedURI = createBlankNode();
		setLabelOnNode(movieTheMatrixReloadedURI, "Movie");
		addProperty(movieTheMatrixReloadedURI, "title", "The Matrix Reloaded");
		addProperty(movieTheMatrixReloadedURI, "year", "2003-05-07");

		URI movieTheMatrixRevolutionsURI = createBlankNode();
		setLabelOnNode(movieTheMatrixRevolutionsURI, "Movie");
		addProperty(movieTheMatrixRevolutionsURI, "title", "The Matrix Revolutions");
		addProperty(movieTheMatrixRevolutionsURI, "year", "2003-10-27");

		URI actorWilliamDafoeNodeURI = createBlankNode();
		setLabelOnNode(actorWilliamDafoeNodeURI, "Actor");
		addProperty(actorWilliamDafoeNodeURI, "name", "William Dafoe");

		URI actorMichaelNyquistURI = createBlankNode();
		setLabelOnNode(actorMichaelNyquistURI, "Actor");
		addProperty(actorMichaelNyquistURI, "name", "Michael Nyquist");

		URI actorKeanuReevesURI = createBlankNode();
		setLabelOnNode(actorKeanuReevesURI, "Actor");
		addProperty(actorKeanuReevesURI, "name", "Keanu Reeves");

		URI actorLaurenceFishburneURI = createBlankNode();
		setLabelOnNode(actorLaurenceFishburneURI, "Actor");
		addProperty(actorLaurenceFishburneURI, "name", "Laurence Fishburne");

		URI actorCarrieAnneMossURI = createBlankNode();
		setLabelOnNode(actorCarrieAnneMossURI, "Actor");
		addProperty(actorCarrieAnneMossURI, "name", "Carrie-Anne Moss");

		URI directorChadStahelskiURI = createBlankNode();
		setLabelOnNode(directorChadStahelskiURI, "Director");
		addProperty(directorChadStahelskiURI, "name", "Chad Stahelski");

		URI directorDavidLeitchURI = createBlankNode();
		setLabelOnNode(directorDavidLeitchURI, "Director");
		addProperty(directorDavidLeitchURI, "name", "David Leitch");

		URI movieJohnWickURI = createBlankNode();
		setLabelOnNode(movieJohnWickURI, "Movie");
		addProperty(movieJohnWickURI, "title", "John Wick");
		addProperty(movieJohnWickURI, "year", "2014-10-24");

		URI relation = addRelationship(actorKeanuReevesURI, movieTheMatrixURI, "ACTS_IN", null);
		addMetadataToProperty(relation, "role", "Neo");

		relation = addRelationship(actorKeanuReevesURI, movieTheMatrixReloadedURI, "ACTS_IN", null);
		addMetadataToProperty(relation, "role", "Neo");

		relation = addRelationship(actorKeanuReevesURI, movieTheMatrixRevolutionsURI, "ACTS_IN", null);
		addMetadataToProperty(relation, "role", "Neo");

		relation = addRelationship(actorCarrieAnneMossURI, movieTheMatrixURI, "ACTS_IN", null);
		addMetadataToProperty(relation, "role", "Trinity");

		relation = addRelationship(actorCarrieAnneMossURI, movieTheMatrixReloadedURI, "ACTS_IN", null);
		addMetadataToProperty(relation, "role", "Trinity");

		relation = addRelationship(actorCarrieAnneMossURI, movieTheMatrixRevolutionsURI, "ACTS_IN", null);
		addMetadataToProperty(relation, "role", "Trinity");

		relation = addRelationship(actorKeanuReevesURI, movieJohnWickURI, "ACTS_IN", null);
		addMetadataToProperty(relation, "role", "John Wick");

		relation = addRelationship(actorWilliamDafoeNodeURI, movieJohnWickURI, "ACTS_IN", null);
		addMetadataToProperty(relation, "role", "Marcus");

		relation = addRelationship(actorMichaelNyquistURI, movieJohnWickURI, "ACTS_IN", null);
		addMetadataToProperty(relation, "role", "Viggo Tarasov");

		relation = addRelationship(directorChadStahelskiURI, movieJohnWickURI, "DIRECTED", null);
		addMetadataToProperty(relation, "credits", "uncredited");

		relation = addRelationship(directorDavidLeitchURI, movieJohnWickURI, "DIRECTED", null);
		addMetadataToProperty(relation, "credits", "uncredited");

	}

	private static boolean checkIfDatabaseIsAvailable() {
		boolean databaseIsAvailable = false;
		Client webClient = Client.create();
		webClient.addFilter(new HTTPBasicAuthFilter("neo4j", "Elcapitan1011"));
		WebResource webResource = webClient.resource(DB_SERVER_ROOT_URI);
		ClientResponse clientResponse = webResource.get(ClientResponse.class);
		if (clientResponse.getStatus() == 200) {
			System.out.format("Database connection successful");
			databaseIsAvailable = true;
		} else {
			System.out.format("Database is unavailable. Got status code: %d", clientResponse.getStatus());
		}

		return databaseIsAvailable;
	}

	private static URI createBlankNode() throws CustomException {
		final String nodeEntryPointUri = DB_SERVER_ROOT_URI + "node";
		Client webClient = Client.create();
		webClient.addFilter(new HTTPBasicAuthFilter("neo4j", "Elcapitan1011"));
		WebResource webResource = webClient.resource(nodeEntryPointUri);
		ClientResponse clientResponse = webResource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON)
				.entity("{}").post(ClientResponse.class);
		URI nodeURI = clientResponse.getLocation();
		if (clientResponse.getStatus() != 201) {
			throw new CustomException("Unable to create actor node");
		}
		System.out.println("Actor node created successfully");
		return nodeURI;

	}

	private static URI setLabelOnNode(URI nodeURI, String label) throws CustomException {
		final String labelEntryPointURL = nodeURI.toString() + "/labels";
		Client webClient = Client.create();
		webClient.addFilter(new HTTPBasicAuthFilter("neo4j", "Elcapitan1011"));
		WebResource webResource = webClient.resource(labelEntryPointURL);
		ClientResponse clientResponse = webResource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, "[\"" + label + "\" ]");
		if (clientResponse.getStatus() != 204) {
			throw new CustomException("Unable to set label on node");
		}
		System.out.println("Label set on node");
		clientResponse.close();
		return nodeURI;
	}

	private static void addProperty(URI nodeUri, String propertyName, String propertyValue) throws CustomException {
		String propertyURI = nodeUri.toString() + "/properties/" + propertyName;
		Client webClient = Client.create();
		webClient.addFilter(new HTTPBasicAuthFilter("neo4j", "Elcapitan1011"));
		WebResource webResource = webClient.resource(propertyURI);
		ClientResponse clientResponse = webResource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON)
				.entity("\"" + propertyValue + "\"").put(ClientResponse.class);
		if (clientResponse.getStatus() != 204) {
			throw new CustomException(
					"Unable to add properties to the node. received errorcode:" + clientResponse.getStatus());
		}
		System.out.println("Successfully set properties on the node");
		clientResponse.close();
	}

	private static URI addRelationship(URI startNode, URI endNode, String relationshipType, String jsonAttributes)
			throws URISyntaxException, CustomException {
		URI fromUri = new URI(startNode.toString() + "/relationships");
		String relationshipJson = generateJsonRelationship(endNode, relationshipType, jsonAttributes);
		Client webClient = Client.create();
		webClient.addFilter(new HTTPBasicAuthFilter("neo4j", "Elcapitan1011"));
		WebResource resource = webClient.resource(fromUri);
		// POST JSON to the relationships URI
		ClientResponse clientResponse = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON)
				.entity(relationshipJson).post(ClientResponse.class);

		final URI location = clientResponse.getLocation();
		if (clientResponse.getStatus() != 201) {
			throw new CustomException("Unable to create relation. received errorcode:" + clientResponse.getStatus());
		}
		System.out.println("Successfully created relation");

		clientResponse.close();
		return location;
	}

	private static String generateJsonRelationship(URI endNode, String relationshipType, String... jsonAttributes) {
		StringBuilder sb = new StringBuilder();
		sb.append("{ \"to\" : \"");
		sb.append(endNode.toString());
		sb.append("\", ");

		sb.append("\"type\" : \"");
		sb.append(relationshipType);
		if (jsonAttributes == null || jsonAttributes.length < 1) {
			sb.append("\"");
		} else {
			sb.append("\", \"data\" : ");
			for (int i = 0; i < jsonAttributes.length; i++) {
				sb.append(jsonAttributes[i]);
				if (i < jsonAttributes.length - 1) { // Miss off the final comma
					sb.append(", ");
				}
			}
		}

		sb.append(" }");
		return sb.toString();
	}

	private static void addMetadataToProperty(URI relationshipUri, String name, String value)
			throws URISyntaxException, CustomException {
		URI propertyUri = new URI(relationshipUri.toString() + "/properties");
		String entity = toJsonNameValuePairCollection(name, value);
		Client webClient = Client.create();
		webClient.addFilter(new HTTPBasicAuthFilter("neo4j", "Elcapitan1011"));
		WebResource resource = webClient.resource(propertyUri);
		ClientResponse clientResponse = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON)
				.entity(entity).put(ClientResponse.class);
		if (clientResponse.getStatus() != 204) {
			throw new CustomException("Unable to add metadata to property");
		}
		System.out.println("Successfully added metadata to property");
		clientResponse.close();
	}

	private static String toJsonNameValuePairCollection(String name, String value) {
		return String.format("{ \"%s\" : \"%s\" }", name, value);
	}

}
