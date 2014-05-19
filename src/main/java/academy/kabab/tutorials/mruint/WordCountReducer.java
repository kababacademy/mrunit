package academy.kabab.tutorials.mruint;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class WordCountReducer extends
		Reducer<Text, LongWritable, Text, LongWritable> {

	MultipleOutputs<Text, LongWritable> outputs;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		outputs = new MultipleOutputs<Text, LongWritable>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		long counter = 0;
		for (LongWritable value : values) {
			counter = counter + value.get();
		}
		outputs.write(key, new LongWritable(counter), key.toString());
	}
}
