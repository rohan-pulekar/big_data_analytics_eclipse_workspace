package ee6.course.streaming.utils;

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

import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * This is a utility class to write the highway name and speed information to
 * output csv files.
 * 
 * This class is part of final project of e63 course (Big Data Analytics) of
 * Harvard Extension School
 * 
 * @author Rohan Pulekar
 *
 */
public class FileWriterForLocalAndS3 implements Serializable {

	private static final long serialVersionUID = 1L;

	// initialize the logger that is based on slf4j library
	private static final Logger LOGGER = LoggerFactory.getLogger(FileWriterForLocalAndS3.class);

	// a map of date and list of highway info
	private static Map<Date, List<HighwayInfoKafkaMessage>> mapOfTimeAndHighwaySpeeds = new TreeMap<Date, List<HighwayInfoKafkaMessage>>();

	// set of strings which will hold csv file headers
	private static Set<String> csvHeaders = new TreeSet<String>();

	// csv output file path and name
	private static final File csvOutputFile = new File(HighwayInfoConstants.CSV_OUTPUT_FILE_NAME);

	// aws s3 client
	private static final AmazonS3 AWS_S3_CLIENT = new AmazonS3Client(
			new BasicAWSCredentials(HighwayInfoConstants.AWS_ACCESS_KEY_ID, HighwayInfoConstants.AWS_SECRET_KEY));

	// delete object request for deleting csv file on S3 (since this ill not
	// change)
	private static DeleteObjectRequest S3_DELETE_OBJECT_REQUEST = new DeleteObjectRequest(
			HighwayInfoConstants.S3_BUCKET__NAME, HighwayInfoConstants.S3_BUCKET_CSV_FILE_OBJECT);

	// S3 access control list
	private static AccessControlList S3_ACCESS_CONTROl_LIST = new AccessControlList();

	/**
	 * This function will create a new local csv file
	 * 
	 * @throws IOException
	 */
	public static void createNewLocalCSVFile() throws IOException {

		if (csvOutputFile.exists() && csvOutputFile.isFile()) {
			csvOutputFile.delete();
		}
		csvOutputFile.createNewFile();

		mapOfTimeAndHighwaySpeeds.clear();
		csvHeaders.clear();
	}

	/**
	 * This function will add a tuple to the file write buffer
	 * 
	 * @param tupleToBeWrittenToCSV
	 */
	public static void addTupleToFileWriteBuffer(Tuple2<Date, HighwayInfoKafkaMessage> tupleToBeWrittenToCSV) {

		// create a blank list of highway info kafka messages
		List<HighwayInfoKafkaMessage> listOfHighwayInfoKafkaMessages = null;

		// check if MAP_OF_DATE_AND_HIGHWAY_INFO contains the highway record for
		// the given date
		if (mapOfTimeAndHighwaySpeeds.containsKey(tupleToBeWrittenToCSV._1)) {

			// get the date obect (which is the key) from the tuple
			listOfHighwayInfoKafkaMessages = mapOfTimeAndHighwaySpeeds.get(tupleToBeWrittenToCSV._1);

			// if list of highway info kafka messages is blank, then create a
			// new list
			if (listOfHighwayInfoKafkaMessages == null) {
				listOfHighwayInfoKafkaMessages = new ArrayList<HighwayInfoKafkaMessage>(8);
			}
		} else {
			// if MAP_OF_DATE_AND_HIGHWAY_INFO does not contain the highway info
			// record for the given date then create new list of highway info
			// kafka message
			listOfHighwayInfoKafkaMessages = new ArrayList<HighwayInfoKafkaMessage>(8);
		}

		// add the highway info kafka message to the list of highway info kafka
		// messages
		listOfHighwayInfoKafkaMessages.add(tupleToBeWrittenToCSV._2);

		// add the header to the list of headers
		csvHeaders.add(tupleToBeWrittenToCSV._2.getHighway().getDisplayName());

		// add the date and highway info kafka messages list to the
		// mapOfDateAndHighwayInfo
		mapOfTimeAndHighwaySpeeds.put(tupleToBeWrittenToCSV._1, listOfHighwayInfoKafkaMessages);
	}

	/**
	 * flush out the csv file to local and S3 files
	 * 
	 * @throws IOException
	 */
	public static void flushBufferToLocalAndS3Files(SQLContext sqlContext) throws IOException {

		if (mapOfTimeAndHighwaySpeeds.entrySet()
				.size() > HighwayInfoConstants.NUMBER_OF_RECORDS_TO_APPLY_PREDICTION_AFTER) {
			// FutureSpeedPredictorWithSparkML.addSpeedPredictions(mapOfTimeAndHighwaySpeeds,
			// sqlContext);
		}

		// create the file writer for CSV file
		FileWriter csvOutputFileWriter = new FileWriter(csvOutputFile, true);

		// call the function to write csv file headers
		writeCSVFileHeaders(csvOutputFileWriter);

		// run a loop for each entry in date and highway info
		for (Map.Entry<Date, List<HighwayInfoKafkaMessage>> entry : mapOfTimeAndHighwaySpeeds.entrySet()) {

			// write the time to csv file
			csvOutputFileWriter.write(HighwayInfoConstants.DATE_FORMATTER_FOR_DATE_TIME.format(entry.getKey()));

			// run a loop for each highwayInfoKafka message
			for (HighwayInfoKafkaMessage highwayInfoKafkaMessage : entry.getValue()) {
				csvOutputFileWriter.append(',');
				// write the speed to csv file
				csvOutputFileWriter.append(
						HighwayInfoConstants.DECIMAL_FORMAT_WITH_ROUNDING.format(highwayInfoKafkaMessage.getSpeed()));
			}

			// write a newline character to the csv file
			csvOutputFileWriter.append('\n');
		}

		// flush and close the file writer
		csvOutputFileWriter.flush();
		csvOutputFileWriter.close();

		// call the function to delete the csv file from S3
		deleteCSVFileFromS3();

		// upload the newly created csv file to S3
		uploadCSVFileToS3();
	}

	/**
	 * This function writes csv file headers
	 * 
	 * @param csvOutputFileWriter
	 * @throws IOException
	 */
	private static void writeCSVFileHeaders(FileWriter csvOutputFileWriter) throws IOException {
		csvOutputFileWriter.append("Time");
		System.out.println("csvHeaders:" + csvHeaders);
		for (String csvHeader : csvHeaders) {
			csvOutputFileWriter.append(',');
			csvOutputFileWriter.append(csvHeader);
		}
		csvOutputFileWriter.append('\n');
	}

	/**
	 * This function uses AWS SDK to delete csv file from S3
	 */
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
			ase.printStackTrace();
			LOGGER.error("Exception occured in deleteCSVFileFromS3", ase);
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException.");
			System.out.println("Error Message: " + ace.getMessage());
			ace.printStackTrace();
			LOGGER.error("Exception occured in deleteCSVFileFromS3", ace);
		}
	}

	/**
	 * This function uploads CSV file to S3
	 */
	private static void uploadCSVFileToS3() {
		try {

			// create an AWS S3 put object request
			PutObjectRequest putObjectRequest = new PutObjectRequest(HighwayInfoConstants.S3_BUCKET__NAME,
					HighwayInfoConstants.S3_BUCKET_CSV_FILE_OBJECT, csvOutputFile);

			// grant read permission to all users for the new file about to be
			// put into S3
			S3_ACCESS_CONTROl_LIST.grantPermission(GroupGrantee.AllUsers, Permission.Read);

			// set access control list in the put object request
			putObjectRequest.setAccessControlList(S3_ACCESS_CONTROl_LIST);

			// call put object on AWS S3 client
			AWS_S3_CLIENT.putObject(putObjectRequest);
		} catch (AmazonServiceException ase) {
			System.out.println(
					"Caught an AmazonServiceException in uploadCSVFileToS3, which " + "means your request made it "
							+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
			ase.printStackTrace();
			LOGGER.error("Exception occured in uploadCSVFileToS3", ase);
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which " + "means the client encountered "
					+ "an internal error while trying to " + "communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
			ace.printStackTrace();
			LOGGER.error("Exception occured in uploadCSVFileToS3", ace);
		}
	}
}
