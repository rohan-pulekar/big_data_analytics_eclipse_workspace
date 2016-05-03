package e63.course.final_project;

import java.io.File;
import java.io.FileReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class XmlFileTesteer {
	public static void main(String[] args) throws Exception {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		InputSource is = new InputSource(new FileReader(new File("input_xml_files/RTTM_feed.aspx.xml")));

		Document doc = db.parse(is);
		NodeList btDataNodes = doc.getElementsByTagName("btdata");

		if (btDataNodes != null && btDataNodes.getLength() > 0) {
			Element rootElement = (Element) btDataNodes.item(0);
			if (rootElement != null) {
				NodeList travelDataNodelist = rootElement.getElementsByTagName("TRAVELDATA");
				if (travelDataNodelist != null && travelDataNodelist.getLength() > 0) {
					Element travelDataNode = (Element) travelDataNodelist.item(0);
					if (travelDataNode != null) {
						if (travelDataNode.getElementsByTagName("LastUpdated") != null) {
							if (travelDataNode.getElementsByTagName("LastUpdated").item(0) != null) {
								String lastUpdated = travelDataNode.getElementsByTagName("LastUpdated").item(0)
										.getTextContent();
								System.out.println(lastUpdated);
							}
						}
					}
				}
			}
		}
	}
}
