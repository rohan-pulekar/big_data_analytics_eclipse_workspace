package e63.course.final_project;

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

public class XmlFileProcessor {
	public static Map<MassachusettsHighway, Float> processLiveXmlStream() throws Exception {
		Map<MassachusettsHighway, List<Float>> highwayAndSpeedsListMap = new HashMap<MassachusettsHighway, List<Float>>();
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();

		Document doc = db
				.parse(new URL("https://www.massdot.state.ma.us/feeds/traveltimes/RTTM_feed.aspx").openStream());
		NodeList btDataNodes = doc.getElementsByTagName("btdata");

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
									// System.out.print(massacusettsHighway + "
									// ");
									// System.out.print(speed);
									List<Float> listOfSpeeds = new ArrayList<Float>(1);
									if (highwayAndSpeedsListMap.get(massacusettsHighway) != null) {
										listOfSpeeds = highwayAndSpeedsListMap.get(massacusettsHighway);
									}
									listOfSpeeds.add(speed);
									highwayAndSpeedsListMap.put(massacusettsHighway, listOfSpeeds);
									// System.out.println();
								}
							}
						}
					}
				}
			}
		}
		// System.out.println(highwayAndSpeedsListMap);

		Map<MassachusettsHighway, Float> highwayAndSpeedMap = new HashMap<MassachusettsHighway, Float>();
		for (Entry<MassachusettsHighway, List<Float>> entrySet : highwayAndSpeedsListMap.entrySet()) {
			List<Float> listOfSpeeds = entrySet.getValue();
			if (listOfSpeeds != null) {
				float sumOfSpeeds = 0;
				for (Float speed : listOfSpeeds) {
					sumOfSpeeds += speed;
				}
				float avergeSpeed = sumOfSpeeds / (listOfSpeeds.size());
				highwayAndSpeedMap.put(entrySet.getKey(), avergeSpeed);
			}
		}
		// System.out.println(highwayAndSpeedMap);
		return highwayAndSpeedMap;
	}
}
