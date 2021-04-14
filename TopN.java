/*
 * Inspired by this tutorial
 * http://www.philippeadjiman.com/blog/2009/12/20/hadoop-tutorial-series-issue-2-getting-started-with-customized-partitioning/
 */

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;



public class TopN {
	
	public static class TopNMapper extends Mapper<Object, Text, Text, LongWritable> {
		private TreeMap<Long, String> treeMap;
		public void map(Object key, Text value, Context context) {
			String[] arr = value.toString().split("\t");
			String name = arr[0];
			long count = Long.parseLong(arr[1]);
			treeMap.put(count, name);
			
		}
	}
	public static class TopNReducer extends Reducer<Text, LongWritable, LongWritable, Text> {
		private TreeMap<Long, String> tmap;

	}
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,
                                  args).getRemainingArgs();
  
        // if less than two paths 
        // provided will show error
        if (otherArgs.length < 2) 
        {
            System.err.println("Error: please provide two paths");
            System.exit(2);
        }
  
        Job job = Job.getInstance(conf, "top 10");
        job.setJarByClass(TopN.class);
  
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
  
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
  
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
  
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
  
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}