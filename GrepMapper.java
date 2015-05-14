import java.io.IOException;
import java.util.StringTokenizer;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Pattern pattern;
	 
	public void configure(JobConf job) {
		pattern = Pattern.compile(job.get("mapred.mapper.regex"));
	}
	 
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		String line = value.toString();
		Matcher matcher = pattern.matcher(line);
		if (matcher.find()) {
			output.collect(new Text(line), one);
		}
	}

}