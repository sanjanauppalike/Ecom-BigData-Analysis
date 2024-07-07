import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SuccessRate {
	
	public static class SuccessMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] tokens = value.toString().split(",");
			String itemId = tokens[2];
			int clickOrBuy;
			if(tokens.length == 4)
				clickOrBuy = 1;
			else
				clickOrBuy = -1;

			context.write(new Text(itemId), new IntWritable(clickOrBuy));
		}
	}
	
	public static class SuccessReducer extends Reducer<Text, IntWritable, Text, FloatWritable>{
		
		public void reduce(Text itemId, Iterable<IntWritable> clickOrBuy, Context context) throws IOException, InterruptedException {
			int clicks = 0;
			int buys = 0;
			Iterator<IntWritable> iterator = clickOrBuy.iterator();
			while(iterator.hasNext()) {
				int val = iterator.next().get();
				if(val == 1)
					clicks++;
				else
					buys++;
			}
			FloatWritable successRate = new FloatWritable(((float)buys)/clicks);
			context.write(itemId, successRate);
		}
	}
	
	public static class SortMapper extends Mapper<Object, Text, Text, FloatWritable>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			Text itemId = new Text(tokens[0]);
			FloatWritable successRate = new FloatWritable(Float.parseFloat(tokens[1]));
			context.write(itemId, successRate);
		}
	}
	
	public static class SortReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{
		
		private Map<String, Float> revenueMap = new HashMap<String, Float>();
		public void reduce(Text itemId, Iterable<FloatWritable> successRates, Context context) {
			for (FloatWritable successRate: successRates) {
				revenueMap.put(itemId.toString(), successRate.get());
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			Map<String, Float> sortedMap = sort(revenueMap);
	  		  int counter = 0;
	  		  for (String key: sortedMap.keySet()) {
	  			  if (counter ++ == 10) {
	  				  break;
	  			  }
	  			  context.write(new Text(key),new FloatWritable(sortedMap.get(key)));
	  		  }
		}
		
		public static Map<String, Float> sort(Map<String, Float> unsortMap) {

	        List<Map.Entry<String, Float>> list =
	                new LinkedList<Map.Entry<String, Float>>(unsortMap.entrySet());
//	        for (Map.Entry<String, Integer> entry : list) {
//	            System.out.println("-----IN COMPARATOR METHOD------"+entry.getKey()+"  "+entry.getValue());
//	        }
	        Collections.sort(list, new Comparator<Map.Entry<String, Float>>() {
	            public int compare(Map.Entry<String, Float> o1,
	                               Map.Entry<String, Float> o2) {
	            	if(!o2.getValue().equals(o1.getValue()))
	            		return (o2.getValue()).compareTo(o1.getValue());
	            	else
	            		return (o1.getKey().compareTo(o2.getKey()));
	            }
	        });

	        Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
	        for (Map.Entry<String, Float> entry : list) {
	            sortedMap.put(entry.getKey(), entry.getValue());
	        }

	        return sortedMap;
	    }
		
	}
	

	

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length !=2) {
			System.err.println("Usage: input_file output_file");
			System.exit(2);
		}
		
		Job job1 = Job.getInstance(conf, "Success Rate");
		job1.setJarByClass(SuccessRate.class);
		job1.setMapperClass(SuccessMapper.class);
		job1.setReducerClass(SuccessReducer.class);
		job1.setNumReduceTasks(10);
		job1.setOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]+"/temp"));
		
		if (!job1.waitForCompletion(true)) {
			  System.exit(1);
		}
		
		Job job2 = Job.getInstance(conf);
	    job2.setJarByClass(SuccessRate.class);
	    job2.setJobName("sort");
	    job2.setNumReduceTasks(1);

	    FileInputFormat.setInputPaths(job2, new Path(otherArgs[1] + "/temp"));
	    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1] + "/final"));

	    job2.setMapperClass(SortMapper.class);
	    job2.setReducerClass(SortReducer.class);

	    job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(FloatWritable.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(FloatWritable.class);
	    
	    if (!job2.waitForCompletion(true)) {
			  System.exit(1);
		}

	}

}
