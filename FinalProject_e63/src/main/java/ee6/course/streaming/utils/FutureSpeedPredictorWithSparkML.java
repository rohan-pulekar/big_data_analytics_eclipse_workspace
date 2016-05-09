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

public class FutureSpeedPredictorWithSparkML {

	private static final LinearRegression LINEAR_REGRESSION = new LinearRegression();

	private static List<Double> getListOfPredictedSpeeds(
			Map<Date, List<HighwayInfoKafkaMessage>> mapOfTimeAndHighwaySpeeds, SQLContext sqlContext) {

		List<Double> listOfPredictedSpeeds = new ArrayList<Double>(8);

		int columnCounter = 0;
		while (true) {

			List<LabeledPoint> listOfLabelledPoints = new ArrayList<LabeledPoint>();

			int rowCounter = 0;

			// run a loop for each entry in date and highway info
			for (Map.Entry<Date, List<HighwayInfoKafkaMessage>> timeAndHighwayMapEntry : mapOfTimeAndHighwaySpeeds
					.entrySet()) {
				if (columnCounter >= timeAndHighwayMapEntry.getValue().size()) {
					return listOfPredictedSpeeds;
				}
				listOfLabelledPoints
						.add(new LabeledPoint(timeAndHighwayMapEntry.getValue().get(columnCounter).getSpeed(),
								Vectors.dense((double) rowCounter)));

				rowCounter++;
			}
			DataFrame training = sqlContext.createDataFrame(listOfLabelledPoints, LabeledPoint.class);

			LinearRegressionModel linearRegressionModel = LINEAR_REGRESSION.fit(training);

			double predictedSpeed = linearRegressionModel.predict(Vectors.dense(rowCounter));
			listOfPredictedSpeeds.add(predictedSpeed);

			columnCounter++;
		}
	}

	public static void addSpeedPredictions(Map<Date, List<HighwayInfoKafkaMessage>> mapOfTimeAndHighwaySpeeds,
			SQLContext sqlContext) {
		List<Double> listOfPredictedSpeeds = getListOfPredictedSpeeds(mapOfTimeAndHighwaySpeeds, sqlContext);
		List<HighwayInfoKafkaMessage> listOfPredictions = new ArrayList<HighwayInfoKafkaMessage>(8);
		for (Map.Entry<Date, List<HighwayInfoKafkaMessage>> timeAndHighwayMapEntry : mapOfTimeAndHighwaySpeeds
				.entrySet()) {

			int counter = 0;
			for (HighwayInfoKafkaMessage highwayInfoKafkaMessage : timeAndHighwayMapEntry.getValue()) {
				if (counter >= listOfPredictedSpeeds.size()) {
					break;
				}
				float predictedSpeed = (float) (listOfPredictedSpeeds.get(counter).doubleValue());
				HighwayInfoKafkaMessage highwayInfoSpeedPrediction = new HighwayInfoKafkaMessage(
						highwayInfoKafkaMessage.getHighway(), predictedSpeed);
				listOfPredictions.add(highwayInfoSpeedPrediction);
				counter++;
			}
			break;
		}
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.HOUR, -4);
		calendar.add(Calendar.SECOND, HighwayInfoConstants.SLIDE_INTERVAL_IN_SECS);
		Date timeStampOfSpeedPrediction = calendar.getTime();
		mapOfTimeAndHighwaySpeeds.put(timeStampOfSpeedPrediction, listOfPredictions);
		System.out.println("added speed predictions");
	}
}
