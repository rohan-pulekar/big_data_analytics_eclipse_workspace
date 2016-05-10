package ee6.course.streaming.utils;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import e63.course.dtos.MassachusettsHighway;

/**
 * This class is for processing live xml feed. The XML feed that will be
 * processed is Mass DOT live highway speed xml feed
 * 
 * This class is part of final project of e63 course (Big Data Analytics) of
 * Harvard Extension School
 * 
 * @author Rohan Pulekar
 *
 */
public class LiveXmlFeedReader {

	/**
	 * This function processes live xml stream and returns a map of Highway and
	 * speed
	 * 
	 * @return map <highway-name average-speed>
	 * @throws Exception
	 */
	public static Map<MassachusettsHighway, Float> processLiveXmlStream() throws Exception {

		// initalize a map of highway and speeds
		Map<MassachusettsHighway, List<Float>> highwayAndSpeedsListMap = new HashMap<MassachusettsHighway, List<Float>>();

		// create a document builder factory
		DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();

		// create an instance of document builder
		DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();

		// create a document of the current live xml feed
		Document document = documentBuilder
				.parse(new URL(HighwayInfoConstants.MASS_DOT_LIVE_XML_FEED_LINK).openStream());

		// start processing of the xml document to get average speed on the
		// highway
		NodeList btDataNodes = document.getElementsByTagName("btdata");

		if (btDataNodes != null && btDataNodes.getLength() > 0) {
			Element rootElement = (Element) btDataNodes.item(0);
			if (rootElement != null) {
				NodeList travelDataNodelist = rootElement.getElementsByTagName("TRAVELDATA");
				if (travelDataNodelist != null && travelDataNodelist.getLength() > 0
						&& travelDataNodelist.item(0) != null) {

					Element travelDataNode = (Element) travelDataNodelist.item(0);

					NodeList pairDataNodeList = travelDataNode.getElementsByTagName("PAIRDATA");
					if (pairDataNodeList != null) {
						for (int pairDataNodeCounter = 0; pairDataNodeCounter < pairDataNodeList
								.getLength(); pairDataNodeCounter++) {
							Element pairDataNode = (Element) pairDataNodeList.item(pairDataNodeCounter);
							MassachusettsHighway massacusettsHighway = null;
							NodeList pairIDNodesList = pairDataNode.getElementsByTagName("PairID");
							if (pairIDNodesList != null && pairIDNodesList.getLength() > 0
									&& pairIDNodesList.item(0) != null) {
								int pairId = Integer.parseInt(pairIDNodesList.item(0).getTextContent());
								massacusettsHighway = HighwayInfoConstants.highWayXMLFeedMap.get(pairId);
								if (massacusettsHighway == null) {
									continue;
								}
							}

							NodeList speedNodesList = pairDataNode.getElementsByTagName("Speed");
							if (speedNodesList != null && speedNodesList.getLength() > 0
									&& speedNodesList.item(0) != null) {
								String speedAsString = speedNodesList.item(0).getTextContent();
								if (StringUtils.isNotBlank(speedAsString) && NumberUtils.isNumber(speedAsString)) {
									float speed = Float.parseFloat(speedAsString);
									List<Float> listOfSpeeds = new ArrayList<Float>(1);
									if (highwayAndSpeedsListMap.get(massacusettsHighway) != null) {
										listOfSpeeds = highwayAndSpeedsListMap.get(massacusettsHighway);
									}
									listOfSpeeds.add(speed);
									highwayAndSpeedsListMap.put(massacusettsHighway, listOfSpeeds);
								}
							}
						}
					}
				}
			}
		}

		// create map of highway and speed
		Map<MassachusettsHighway, Float> highwayAndCurrentAvgSpeedMap = new HashMap<MassachusettsHighway, Float>();

		// loop over the highway and speeds list map
		for (Entry<MassachusettsHighway, List<Float>> entrySet : highwayAndSpeedsListMap.entrySet()) {

			// get the list of numbers denoting speed for that highway
			List<Float> listOfSpeeds = entrySet.getValue();

			if (listOfSpeeds != null) {
				float sumOfSpeeds = 0;
				for (Float speed : listOfSpeeds) {
					sumOfSpeeds += speed;
				}

				// get the average speed
				float avergeSpeed = sumOfSpeeds / (listOfSpeeds.size());

				// put into the map the highway name and average speed
				highwayAndCurrentAvgSpeedMap.put(entrySet.getKey(), avergeSpeed);
			}
		}

		// return the highway and averge speed map
		return highwayAndCurrentAvgSpeedMap;
	}
}
