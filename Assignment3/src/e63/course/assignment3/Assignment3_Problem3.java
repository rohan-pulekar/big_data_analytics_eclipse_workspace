
package e63.course.assignment3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Assignment3_Problem3 {

	private static class Problem3Mapper extends Mapper<Text, Text, IntWritable, IntWritable> {

		IntWritable wordCount = new IntWritable();
		IntWritable one = new IntWritable(1);

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			try {
				// the below String.valueOf function means that even if value is
				// null, the wordCount will be considered as 0.
				wordCount.set(Integer.parseInt(String.valueOf(value)));
			} catch (NumberFormatException nfe) {
				wordCount.set(0);
			}
			// the below is done so that wordcount becomes the key and the word
			// itself becomes the value
			context.write(wordCount, one);
		}
	}

	private static class Problem3Reducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		IntWritable numberOfWords = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// this will count the number of words for a particular word-count
			int numberOfWordsCounter = 0;
			for (IntWritable val : values) {
				numberOfWordsCounter = numberOfWordsCounter + val.get();
			}
			numberOfWords.set(numberOfWordsCounter);
			// here key=word-count. value=number Of Words which have that word
			// count
			context.write(key, numberOfWords);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: Assignment3_Problem3 <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Assignment3_Problem3");
		job.setJarByClass(Assignment3_Problem3.class);
		job.setMapperClass(Problem3Mapper.class);
		job.setCombinerClass(Problem3Reducer.class);
		job.setReducerClass(Problem3Reducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
