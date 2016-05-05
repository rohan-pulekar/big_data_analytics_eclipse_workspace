package e63.course.final_project;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import e63.course.dtos.MassachusettsHighway;

public class HighwayInfoConstants {

	public static final String CSV_OUTPUT_FILE_NAME = "output_files/highway_info.csv";
	public static final DateFormat DATE_FORMATTER_FOR_TIME = new SimpleDateFormat("hh:mm a z");
	public static final DecimalFormat DECIMAL_FORMAT_WITH_ROUNDING = new DecimalFormat("#.#");
	public static Map<Integer, MassachusettsHighway> highWayXMLFeedMap = new HashMap<Integer, MassachusettsHighway>();

	static {

		highWayXMLFeedMap.put(10088, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(13496, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10082, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10085, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10238, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10086, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10083, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10374, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10497, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10382, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10357, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10356, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10496, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10498, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10499, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10360, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(18931, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10361, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10375, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10376, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(18901, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10379, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(13657, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10389, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10386, MassachusettsHighway.I_90);
		highWayXMLFeedMap.put(10385, MassachusettsHighway.I_90);

		highWayXMLFeedMap.put(5546, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5506, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(18913, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(10192, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(18912, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5508, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5502, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(19627, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5551, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5509, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5552, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(20246, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(10525, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5556, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5510, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5557, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5558, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5511, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5559, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5560, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5498, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(14798, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5561, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5572, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(14796, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5495, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5573, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5574, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5583, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5491, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5575, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5587, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5490, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5588, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5501, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5553, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5554, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(5500, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(16979, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(13663, MassachusettsHighway.I_93);
		highWayXMLFeedMap.put(20247, MassachusettsHighway.I_93);

		highWayXMLFeedMap.put(5532, MassachusettsHighway.I_95);
		highWayXMLFeedMap.put(5533, MassachusettsHighway.I_95);
		highWayXMLFeedMap.put(5569, MassachusettsHighway.I_95);
		highWayXMLFeedMap.put(5568, MassachusettsHighway.I_95);

		highWayXMLFeedMap.put(5583, MassachusettsHighway.I_495);
		highWayXMLFeedMap.put(5584, MassachusettsHighway.I_495);
		highWayXMLFeedMap.put(10110, MassachusettsHighway.I_495);

		highWayXMLFeedMap.put(10179, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(10180, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(10181, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(10183, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(24629, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(24649, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(10184, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(14705, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(10182, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(10185, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(10186, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(18728, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(10187, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(18724, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(10188, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(10189, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(10190, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(10245, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(14714, MassachusettsHighway.ROUTE_3);
		highWayXMLFeedMap.put(14721, MassachusettsHighway.ROUTE_3);

		highWayXMLFeedMap.put(14718, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14750, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14745, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14698, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14748, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14717, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14697, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14747, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14696, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14692, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14722, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14693, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(17711, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14687, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14686, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14690, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14691, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(19257, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14689, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14760, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14688, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14684, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(19186, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14685, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14683, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14682, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14681, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14680, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14679, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(19256, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14695, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(14694, MassachusettsHighway.ROUTE_6);
		highWayXMLFeedMap.put(17712, MassachusettsHighway.ROUTE_6);

		highWayXMLFeedMap.put(24833, MassachusettsHighway.ROUTE_9);
		highWayXMLFeedMap.put(24834, MassachusettsHighway.ROUTE_9);
		highWayXMLFeedMap.put(24835, MassachusettsHighway.ROUTE_9);
		highWayXMLFeedMap.put(24836, MassachusettsHighway.ROUTE_9);

		highWayXMLFeedMap.put(14671, MassachusettsHighway.ROUTE_25);
		highWayXMLFeedMap.put(14673, MassachusettsHighway.ROUTE_25);
		highWayXMLFeedMap.put(14672, MassachusettsHighway.ROUTE_25);
		highWayXMLFeedMap.put(14674, MassachusettsHighway.ROUTE_25);

		highWayXMLFeedMap.put(14709, MassachusettsHighway.ROUTE_28);
		highWayXMLFeedMap.put(14710, MassachusettsHighway.ROUTE_28);
		highWayXMLFeedMap.put(14677, MassachusettsHighway.ROUTE_28);
		highWayXMLFeedMap.put(14708, MassachusettsHighway.ROUTE_28);
		highWayXMLFeedMap.put(14678, MassachusettsHighway.ROUTE_28);
		highWayXMLFeedMap.put(14711, MassachusettsHighway.ROUTE_28);

	}
}
