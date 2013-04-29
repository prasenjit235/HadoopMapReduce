package org.examples.hadoop.mapreduce;

import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CarAnalysisReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	protected void reduce(
			Text arg0,
			java.lang.Iterable<IntWritable> arg1,
			org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>.Context arg2)
			throws java.io.IOException, InterruptedException {
		Iterator<IntWritable> iterator = arg1.iterator();
		int numberOfCars = 0;

		while (iterator.hasNext()) {
			IntWritable intWritable = iterator.next();
			numberOfCars += intWritable.get();
		}
		arg2.write(arg0, new IntWritable(numberOfCars));
	};
}
