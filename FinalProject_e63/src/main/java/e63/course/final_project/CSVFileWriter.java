package e63.course.final_project;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;

import e63.course.dtos.HighwayInfoKafkaMessage;
import scala.Tuple2;

public class CSVFileWriter implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Map<Date, List<HighwayInfoKafkaMessage>> mapOfDateAndHighwayInfo = new TreeMap<Date, List<HighwayInfoKafkaMessage>>();

	private static final Set<String> csvHeaders = new TreeSet<String>();

	private static final File csvOutputFile = new File(HighwayInfoConstants.CSV_OUTPUT_FILE_NAME);

	private static final AmazonS3 AWS_S3_CLIENT = new AmazonS3Client(
			new BasicAWSCredentials(HighwayInfoConstants.AWS_ACCESS_KEY_ID, HighwayInfoConstants.AWS_SECRET_KEY));

	private static final DeleteObjectRequest S3_DELETE_OBJECT_REQUEST = new DeleteObjectRequest(
			HighwayInfoConstants.S3_BUCKET__NAME, HighwayInfoConstants.S3_BUCKET_CSV_FILE_OBJECT);

	private static AccessControlList S3_ACCESS_CONTROl_LIST = new AccessControlList();

	public static void createNewCSVFile() throws IOException {

		if (csvOutputFile.exists() && csvOutputFile.isFile()) {
			csvOutputFile.delete();
		}
		csvOutputFile.createNewFile();

		mapOfDateAndHighwayInfo.clear();
		csvHeaders.clear();
	}

	public static void addToContentsToBeWrittenToCSV(Tuple2<Date, HighwayInfoKafkaMessage> tupleToBeWrittenToCSV) {
		List<HighwayInfoKafkaMessage> listOfHighwayInfoKafkaMessages = null;
		if (mapOfDateAndHighwayInfo.containsKey(tupleToBeWrittenToCSV._1)) {
			listOfHighwayInfoKafkaMessages = mapOfDateAndHighwayInfo.get(tupleToBeWrittenToCSV._1);
			if (listOfHighwayInfoKafkaMessages == null) {
				listOfHighwayInfoKafkaMessages = new ArrayList<HighwayInfoKafkaMessage>(8);
			}
		} else {
			listOfHighwayInfoKafkaMessages = new ArrayList<HighwayInfoKafkaMessage>(8);
		}
		listOfHighwayInfoKafkaMessages.add(tupleToBeWrittenToCSV._2);
		csvHeaders.add(tupleToBeWrittenToCSV._2.getHighway().getDisplayName());
		mapOfDateAndHighwayInfo.put(tupleToBeWrittenToCSV._1, listOfHighwayInfoKafkaMessages);
	}

	public static void writeToCSVFile() throws IOException {
		FileWriter csvOutputFileWriter = new FileWriter(csvOutputFile, true);
		writeCSVFileHeaders(csvOutputFileWriter);
		for (Map.Entry<Date, List<HighwayInfoKafkaMessage>> entry : mapOfDateAndHighwayInfo.entrySet()) {
			csvOutputFileWriter.write(HighwayInfoConstants.DATE_FORMATTER_FOR_DATE_TIME.format(entry.getKey()));
			for (HighwayInfoKafkaMessage highwayInfoKafkaMessage : entry.getValue()) {
				csvOutputFileWriter.append(',');
				csvOutputFileWriter.append(
						HighwayInfoConstants.DECIMAL_FORMAT_WITH_ROUNDING.format(highwayInfoKafkaMessage.getSpeed()));
			}
			csvOutputFileWriter.append('\n');
		}
		csvOutputFileWriter.flush();
		csvOutputFileWriter.close();
		deleteCSVFileFromS3();
		uploadCSVFileToS3();
	}

	private static void writeCSVFileHeaders(FileWriter csvOutputFileWriter) throws IOException {
		csvOutputFileWriter.append("Time");
		System.out.println("csvHeaders:" + csvHeaders);
		for (String csvHeader : csvHeaders) {
			csvOutputFileWriter.append(',');
			csvOutputFileWriter.append(csvHeader);
		}
		csvOutputFileWriter.append('\n');
	}

	private static void deleteCSVFileFromS3() {
		try {
			AWS_S3_CLIENT.deleteObject(S3_DELETE_OBJECT_REQUEST);
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	private static void uploadCSVFileToS3() {
		try {
			PutObjectRequest putObjectRequest = new PutObjectRequest(HighwayInfoConstants.S3_BUCKET__NAME,
					HighwayInfoConstants.S3_BUCKET_CSV_FILE_OBJECT, csvOutputFile);
			S3_ACCESS_CONTROl_LIST.grantPermission(GroupGrantee.AllUsers, Permission.Read);
			putObjectRequest.setAccessControlList(S3_ACCESS_CONTROl_LIST);
			AWS_S3_CLIENT.putObject(putObjectRequest);
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which " + "means your request made it "
					+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which " + "means the client encountered "
					+ "an internal error while trying to " + "communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	public static void main(String args[]) {
		deleteCSVFileFromS3();
		uploadCSVFileToS3();
	}
}
