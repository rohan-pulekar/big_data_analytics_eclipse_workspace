package e63.course.assignment3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Assignment3_Problem5NewAPI extends Configured implements Tool {
	public static class Problem5InverterMapper extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(value, key);
		}
	}

	public static class Problem5Reducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String csv = "";
			for (Text value : values) {
				if (csv.length() > 0)
					csv += ",";
				csv += value.toString();
			}
			context.write(key, new Text(csv));
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		conf.set("key.value.separator.in.input.line", ",");

		Job job = Job.getInstance(conf, "Assignment3_Problem5NewAPI");

		Path in = new Path(arg0[0]);
		Path out = new Path(arg0[1]);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setJarByClass(Assignment3_Problem3.class);
		job.setMapperClass(Problem5InverterMapper.class);
		job.setCombinerClass(Problem5Reducer.class);
		job.setReducerClass(Problem5Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Assignment3_Problem5NewAPI(), args);

		System.exit(res);
	}
}
