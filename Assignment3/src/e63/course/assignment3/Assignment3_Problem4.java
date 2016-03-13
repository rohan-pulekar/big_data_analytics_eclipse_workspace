package e63.course.assignment3;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Assignment3_Problem4 extends Configured implements Tool {
	private static class Problem1Mapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private static String[] STOP_WORDS = new String[] { "i", "a", "about", "an", "are", "as", "at", "be", "by",
				"com", "for", "from", "how", "in", "is", "it", "of", "on", "or", "that", "the", "this", "to", "was",
				"what", "when", "where", "who", "will", "with", "the", "www" };
		List<String> stopWordsList = Arrays.<String>asList(STOP_WORDS);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// validate the input
			if (value == null) {
				// this means the line read is a blank/empty line.
				// In that case, there is no tokenization/analysis
				// that we can do on it
				return;
			}

			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String str = itr.nextToken();
				if (str == null) {
					continue;
				}

				// will exclude special characters from the final output
				str = str.replaceAll("[^A-Za-z0-9]", "");
				if (str.isEmpty()) {
					continue;
				}

				// will exclude stop words from the final output
				if (stopWordsList.contains(str.toLowerCase())) {
					continue;
				}
				word.set(str);
				context.write(word, one);
			}
		}
	}

	private static class Problem1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

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

	private Job createProblem1Job(Configuration conf, Path input, Path out) throws IOException {
		Job problem1Job = Job.getInstance(conf, "Problem1-job");
		problem1Job.setJarByClass(Assignment3_Problem1.class);
		problem1Job.setMapperClass(Problem1Mapper.class);
		problem1Job.setCombinerClass(Problem1Reducer.class);
		problem1Job.setReducerClass(Problem1Reducer.class);
		problem1Job.setOutputKeyClass(Text.class);
		problem1Job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(problem1Job, input);
		FileOutputFormat.setOutputPath(problem1Job, out);
		return problem1Job;
	}

	private Job createProblem3Job(Configuration conf, Path input, Path out) throws IOException {
		Job problem3Job = Job.getInstance(conf, "Problem3-job");
		problem3Job.setJarByClass(Assignment3_Problem3.class);
		problem3Job.setMapperClass(Problem3Mapper.class);
		problem3Job.setCombinerClass(Problem3Reducer.class);
		problem3Job.setReducerClass(Problem3Reducer.class);
		problem3Job.setMapOutputKeyClass(IntWritable.class);
		problem3Job.setMapOutputValueClass(IntWritable.class);
		problem3Job.setOutputKeyClass(IntWritable.class);
		problem3Job.setOutputValueClass(IntWritable.class);
		problem3Job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(problem3Job, input);
		FileOutputFormat.setOutputPath(problem3Job, out);
		return problem3Job;
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Path input = new Path(args[0]);
		Path out = new Path(args[1]);
		Path temp_dir = new Path("assignment3_problem4_temp_dir");
		Job problem1Job = createProblem1Job(conf, input, temp_dir);
		Job problem3Job = createProblem3Job(conf, temp_dir, out);
		problem1Job.waitForCompletion(true);
		boolean success = problem3Job.waitForCompletion(true);
		cleanup(temp_dir, conf);
		return success ? 1 : 0;
	}

	private void cleanup(Path temp, Configuration conf) throws IOException {
		FileSystem fs = temp.getFileSystem(conf);
		fs.delete(temp, true);
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Assignment3_Problem4 <in> <out>");
			System.exit(2);
		}
		ToolRunner.run(new Configuration(), new Assignment3_Problem4(), args);
	}
}
