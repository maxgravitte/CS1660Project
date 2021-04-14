import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.lang.System;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;



//Code based off of 
//https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Example:_WordCount_v2.0
public class Engine {
	//Map changed to OBJ,text,text int->text to store the document ID, otherwise it will be one big doc.
	public static class EngineMapper extends Mapper<Object, Text, Text, Text>{
		private boolean caseSensitive = false;
		private Text word = new Text();
		static enum CountersEnum { INPUT_WORDS }
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
			StringTokenizer itr = new StringTokenizer(line);
			//final IntWritable textID = new IntWritable(1);
			String DocId = value.toString();
			
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, new Text(DocId));
				Counter counter = context.getCounter(CountersEnum.class.getName(),
				CountersEnum.INPUT_WORDS.toString());
				counter.increment(1);
			}
		}
	}
	//Reduce
	public static class EngineReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Iterator<Text> iteration = values.iterator();
			HashMap<String, Integer> docIDnum = new HashMap<String, Integer>();
			int sum = 0;
			while (iteration.hasNext()) {
				String docID = iteration.next().toString();
				if (!docIDnum.containsKey(docID)){
					docIDnum.put(docID, 1 );
					sum++;
				} else {
					docIDnum.put(docID, 1 + docIDnum.get(docID));
					sum++;
				}
			}
			StringBuilder conVal = new StringBuilder();
			Iterator newIteration = docIDnum.entrySet().iterator();
			conVal.append(sum);

			while (newIteration.hasNext()) {
				Map.Entry keyval = (Map.Entry)newIteration.next();
				conVal.append("\t" + keyval.getValue());
			}
			context.write(key, new Text(conVal.toString()));
		}
	}
	//Main method accepts path to input and path to output
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {	
		Configuration conf = new Configuration();
	    //GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
	    //String[] remainingArgs = optionParser.getRemainingArgs();
	    //if ((remainingArgs.length != 2)) {
	     // System.err.println("Usage: engine <in> <out>]");
	    //  System.exit(2);
	    //}
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(Engine.class);
	    job.setMapperClass(EngineMapper.class);
	    job.setReducerClass(EngineReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.waitForCompletion(true);
	}
}
