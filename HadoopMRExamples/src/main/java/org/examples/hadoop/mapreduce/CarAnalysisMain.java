package org.examples.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CarAnalysisMain {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration configuration = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: CarAnalysisMain <in> <out>");
			System.exit(-1);
		}
		configuration.set(
				"mapreduce.input.keyvaluelinerecordreader.key.value.separator",
				",");

		Job job = new Job(configuration, "Calculate Number Of Cars");
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(CarAnalysisMain.class);

		job.setMapperClass(CarAnalysisMapper.class);
		job.setCombinerClass(CarAnalysisReducer.class);
		job.setReducerClass(CarAnalysisReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : -1);
	}
}
