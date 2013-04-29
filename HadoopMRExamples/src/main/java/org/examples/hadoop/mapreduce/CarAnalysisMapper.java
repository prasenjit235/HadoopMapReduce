package org.examples.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CarAnalysisMapper extends Mapper<Text, Text, Text, IntWritable> {
	protected void map(
			Text key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<Text, Text, Text, IntWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		IntWritable intWritable = new IntWritable();
		Text countryCode =new Text("Total Cars");
		if (isValid(value)) {
			intWritable.set(Integer.parseInt(value.toString()));
		}
		context.write(countryCode, intWritable);
	};

	private boolean isValid(final Text text) {
		if (text != null) {
			int i = Integer.parseInt(text.toString());
			if (i == (int) i && i > 0) {
				return true;
			}
		}
		return false;
	}
}
