package academy.kabab.tutorials.mruint.multi.outputformat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import academy.kabab.tutorials.mruint.WordCountMapper;
import academy.kabab.tutorials.mruint.WordCountReducer;

public class WordCountTest {

	/**
	 * This ouput format tracks the creation of record writers based on base
	 * name specified using MultipleOutputs. When Reducer calls
	 * MultipleOutputs.write method passing the output name, MultipleOutputs
	 * checks if there is any Record Writer for that output name. If there is
	 * none then it will call FileOutputFormat.setOutputName to set the output
	 * name and then create new instance of FileOutputFormat based on the the
	 * configuration. Then it will get the record writer from this output format
	 * which will be used to write the records corrsponding to that output name.
	 */
	public static class TestTextOutputFormat extends
			TextOutputFormat<Text, LongWritable> {

		/**
		 * Stores output name to record writer mapping
		 */
		public static Map<String, RecordWriter<Text, LongWritable>> map = new TreeMap<String, RecordWriter<Text, LongWritable>>();

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.hadoop.mapreduce.lib.output.TextOutputFormat#getRecordWriter
		 * (org.apache.hadoop.mapreduce.TaskAttemptContext)
		 */
		@Override
		public RecordWriter<Text, LongWritable> getRecordWriter(
				TaskAttemptContext job) throws IOException,
				InterruptedException {
			// MultipleOutputs has already modified the file output format
			// output name before calling this method.
			String outputName = FileOutputFormat.getOutputName(job);
			// create new record reader. We implemented custom so that we can
			// store the output name to output key value mapping for testing
			// later on.
			RecordWriter<Text, LongWritable> recordWriter = new TextRecordWriter(
					outputName);
			map.put(outputName, recordWriter);
			return recordWriter;
		}

		/**
		 * Utility method to access the output name to record writer mapping
		 * 
		 * @return
		 */
		public Map<String, RecordWriter<Text, LongWritable>> getBaseNameToRecordWriterMap() {
			return map;
		}

		/**
		 * Customer implementation of record writer to store output name to
		 * output key and value mapping
		 * 
		 */
		public class TextRecordWriter extends RecordWriter<Text, LongWritable> {

			/**
			 * base name or output name
			 */
			private String baseName;

			/**
			 * outputs passed by reducer
			 */
			private List<Pair<Text, LongWritable>> outputs = new ArrayList<Pair<Text, LongWritable>>();

			public TextRecordWriter(String baseName) {
				this.baseName = baseName;
			}

			@Override
			public void write(Text key, LongWritable value) throws IOException,
					InterruptedException {
				outputs.add(new Pair<Text, LongWritable>(key, value));
			}

			@Override
			public void close(TaskAttemptContext context) throws IOException,
					InterruptedException {
				// DO nothing
			}

			/**
			 * @return output name
			 */
			public String getBaseName() {
				return baseName;
			}

			/**
			 * @return outputs
			 */
			public List<Pair<Text, LongWritable>> getOutputs() {
				return outputs;
			}

		}

	}

	@Test
	public void testMapper() throws Exception {
		MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver = MapReduceDriver
				.newMapReduceDriver(new WordCountMapper(),
						new WordCountReducer())
				.withInput(new LongWritable(1), new Text("hello"))
				.withInput(new LongWritable(2), new Text("world"));
		Configuration configuration = mapReduceDriver.getConfiguration();
		// MultipleOutputs will look at following configuration to create the
		// instance of OutputFormat class.
		configuration.set("mapreduce.outputformat.class",
				TestTextOutputFormat.class.getName());
		mapReduceDriver.runTest();
		// write custom assertion code to validate the output

	}
}
