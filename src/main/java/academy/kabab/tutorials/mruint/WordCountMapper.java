package academy.kabab.tutorials.mruint;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String str = value.toString();
		String[] words = str.split("\\s");
		for (int i = 0; i < words.length; i++) {
			context.write(new Text(words[i]), new LongWritable(1));
		}
	}
}
