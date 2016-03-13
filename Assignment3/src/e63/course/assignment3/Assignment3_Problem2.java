
package e63.course.assignment3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Assignment3_Problem2 {

	private static class Problem2Mapper extends Mapper<Text, Text, IntWritable, Text> {

		IntWritable wordCount = new IntWritable();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			// validate the input
			if (key == null) {
				// this means the line read is a blank/empty line.
				// In that case, there is no tokenization/analysis
				// that we can do on it
				return;
			}

			try {
				// the below String.valueOf function means that even if value is
				// null, the wordCount will be considered as 0.
				wordCount.set(Integer.parseInt(String.valueOf(value)));
			} catch (NumberFormatException nfe) {
				wordCount.set(0);
			}
			// the below is done so that wordcount becomes the key and the word
			// itself becomes the value
			context.write(wordCount, key);
		}
	}

	private static class Problem2Reducer extends Reducer<IntWritable, Text, Text, IntWritable> {

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				// this inverts back the sequence of key value
				context.write(value, key);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: Assignment3_Problem2 <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Assignment3_Problem2");
		job.setJarByClass(Assignment3_Problem2.class);
		job.setMapperClass(Problem2Mapper.class);
		job.setReducerClass(Problem2Reducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setSortComparatorClass(DescendingKeyComparator.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class DescendingKeyComparator extends WritableComparator {
		protected DescendingKeyComparator() {
			super(IntWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntWritable key1 = (IntWritable) w1;
			IntWritable key2 = (IntWritable) w2;
			return -1 * key1.compareTo(key2);
		}
	}
}
