package BigDataFinalProject.WordCount;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			Text wordOut = new Text();
			IntWritable one = new IntWritable(1);
			while (itr.hasMoreTokens()) {
				wordOut.set(itr.nextToken());
				context.write(wordOut, one);
			}
		}
	}

	public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text term, Iterable<IntWritable> ones, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			Iterator<IntWritable> iterator = ones.iterator();
			while (iterator.hasNext()) {
				count++;
				iterator.next();

			}
			IntWritable output = new IntWritable(count);
			context.write(term, output);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage : Wordcount <Input_file> <output_directory>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(SumReducer.class);
		job.setNumReduceTasks(10);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean status = job.waitForCompletion(true);
		if (status) {
			System.exit(0);
		} else {
			System.exit(1);
		}
	}
}
