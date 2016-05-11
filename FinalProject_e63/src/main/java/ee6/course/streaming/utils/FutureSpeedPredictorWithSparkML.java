package ee6.course.streaming.utils;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import e63.course.dtos.HighwayInfoKafkaMessage;

/**
 * This class is for predicting future speed given the past speed information.
 * 
 * This class is part of final project of e63 course (Big Data Analytics) of
 * Harvard Extension School
 * 
 * @author Rohan Pulekar
 *
 */
public class FutureSpeedPredictorWithSparkML {

	// create a linear regression instance
	private static final LinearRegression LINEAR_REGRESSION = new LinearRegression();

	/**
	 * This function predicts speed of after N minutes, given the speed
	 * information of last N*12 minutes
	 * 
	 * @param mapOfTimeAndHighwaySpeeds
	 * @param sqlContext
	 * @return list of preicted speeds
	 */
	private static List<Double> getListOfPredictedSpeeds(
			Map<Date, List<HighwayInfoKafkaMessage>> mapOfTimeAndHighwaySpeeds, SQLContext sqlContext) {

		// create a list for predcited speeds
		List<Double> listOfPredictedSpeeds = new ArrayList<Double>(8);

		// set a column counter
		int columnCounter = 0;

		// run a loop
		while (true) {

			// create a list of labelled points for linear regression prediction
			List<LabeledPoint> listOfLabelledPoints = new ArrayList<LabeledPoint>();

			// create a row counter
			int rowCounter = 0;

			// run a loop for each entry in date and highway info
			for (Map.Entry<Date, List<HighwayInfoKafkaMessage>> timeAndHighwayMapEntry : mapOfTimeAndHighwaySpeeds
					.entrySet()) {

				// the below if condition is true when all columns of current
				// row are read
				if (columnCounter >= timeAndHighwayMapEntry.getValue().size()) {
					// return the predicted speeds
					return listOfPredictedSpeeds;
				}

				// add a new label point with speed as the label
				listOfLabelledPoints
						.add(new LabeledPoint(timeAndHighwayMapEntry.getValue().get(columnCounter).getSpeed(),
								Vectors.dense((double) rowCounter)));

				// increment the row counter
				rowCounter++;
			}

			// create a dataframe for training
			DataFrame training = sqlContext.createDataFrame(listOfLabelledPoints, LabeledPoint.class);

			// fit the training data frame into a linear regression model
			LinearRegressionModel linearRegressionModel = LINEAR_REGRESSION.fit(training);

			// predict the speed based on the trained model
			double predictedSpeed = linearRegressionModel.predict(Vectors.dense(rowCounter));

			// add the predicted speed to list of predcited speeds
			listOfPredictedSpeeds.add(predictedSpeed);

			// increement the column counter
			columnCounter++;
		}
	}

	/**
	 * This function adds speed predictions to a list of highway and speed
	 * 
	 * @param mapOfTimeAndHighwaySpeeds
	 * @param sqlContext
	 */
	public static void addSpeedPredictions(Map<Date, List<HighwayInfoKafkaMessage>> mapOfTimeAndHighwaySpeeds,
			SQLContext sqlContext) {

		// call the below function to predict speeds based on the current
		// information
		List<Double> listOfPredictedSpeeds = getListOfPredictedSpeeds(mapOfTimeAndHighwaySpeeds, sqlContext);

		// create a list for predictions
		List<HighwayInfoKafkaMessage> listOfPredictions = new ArrayList<HighwayInfoKafkaMessage>(8);

		// loop through each entry in time and highway map
		for (Map.Entry<Date, List<HighwayInfoKafkaMessage>> timeAndHighwayMapEntry : mapOfTimeAndHighwaySpeeds
				.entrySet()) {

			// create a counter
			int counter = 0;

			// loop through each highway infor kafka message for a particular
			// time
			for (HighwayInfoKafkaMessage highwayInfoKafkaMessage : timeAndHighwayMapEntry.getValue()) {

				// check if all highway info records for the given time are
				// processed
				if (counter >= listOfPredictedSpeeds.size()) {
					break;
				}

				// get the predicted speed from the list
				float predictedSpeed = (float) (listOfPredictedSpeeds.get(counter).doubleValue());

				// create a new highway kafka message containing the predicted
				// speed
				HighwayInfoKafkaMessage highwayInfoSpeedPrediction = new HighwayInfoKafkaMessage(
						highwayInfoKafkaMessage.getHighway(), predictedSpeed);

				// add the new highway info kafka message to the list of
				// predictions
				listOfPredictions.add(highwayInfoSpeedPrediction);

				// increement the counter
				counter++;
			}
			// we only need information from one row
			break;
		}

		// get the date instance for prediction
		Calendar calendar = Calendar.getInstance();
		// calendar.add(Calendar.HOUR, -4);
		calendar.add(Calendar.SECOND, HighwayInfoConstants.SLIDE_INTERVAL_IN_SECS);
		Date timeStampOfSpeedPrediction = calendar.getTime();

		// add the list of predictions to the map of time and highway speeds
		mapOfTimeAndHighwaySpeeds.put(timeStampOfSpeedPrediction, listOfPredictions);

		// print out a message indicating prediction successful
		System.out.println("added speed predictions");
	}
}
