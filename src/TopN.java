/*
 * Inspired by this tutorial
 * http://www.philippeadjiman.com/blog/2009/12/20/hadoop-tutorial-series-issue-2-getting-started-with-customized-partitioning/
 */

import java.io.IOException;
import java.util.Map;
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
		static int n;
		@Override
	    public void setup(Context context) throws IOException,InterruptedException
	    {
			Configuration conf = context.getConfiguration();
			String param = conf.get("topN");
			n = Integer.parseInt(param);
			
	        treeMap = new TreeMap<Long, String>();
	    }
		@Override
		public void map(Object key, Text value, Context context) {
			String[] arr = value.toString().split("\t");
			if(arr.length>1) {
				String name = arr[0];
				long count = Long.parseLong(arr[1]);
				treeMap.put(count, name);
			}
			if (treeMap.size()>n) {
				treeMap.remove(treeMap.firstKey());
			}
		}
		@Override
	    public void cleanup(Context context) throws IOException,
	                                       InterruptedException
	    {
	        for (Map.Entry<Long, String> entry : treeMap.entrySet()) 
	        {
	  
	            long count = entry.getKey();
	            String name = entry.getValue();
	  
	            context.write(new Text(name), new LongWritable(count));
	        }
	    }
	}
	public static class TopNReducer extends Reducer<Text, LongWritable, LongWritable, Text> {
		private TreeMap<Long, String> treeMap2;
		static int n;
		@Override
	    public void setup(Context context) throws IOException,
	                                     InterruptedException
	    {
			treeMap2 = new TreeMap<Long, String>();
			Configuration conf = context.getConfiguration();
			String param = conf.get("topN");
			n = Integer.parseInt(param);
	    }
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			String name = key.toString();
			long count = 0;
			for (LongWritable val : values) {
				count = val.get();
			}
			treeMap2.put(count, name);
			if(treeMap2.size()>n) {
				treeMap2.remove(treeMap2.firstKey());
			}
		}
		@Override
	    public void cleanup(Context context) throws IOException,
	                                       InterruptedException
	    {
	  
	        for (Map.Entry<Long, String> entry : treeMap2.entrySet()) 
	        {
	  
	            long count = entry.getKey();
	            String name = entry.getValue();
	            context.write(new LongWritable(count), new Text(name));
	        }
	    }

	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,
                                  args).getRemainingArgs();
  
        // if less than two paths 
        // provided will show error
        if (otherArgs.length < 3) 
        {
            System.err.println("Error: please provide two paths and N");
            System.exit(2);
        }

        conf.set("topN", otherArgs[2]);
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

        job.waitForCompletion(true);
	}
}