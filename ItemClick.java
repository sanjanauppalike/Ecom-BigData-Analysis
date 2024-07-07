

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ItemClick {
	
	public static class ItemClickMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			Text itemID = new Text();
			IntWritable one = new IntWritable(1);
			String[] tokens = value.toString().split(",");
			String[] timestamp = tokens[1].split("-");
			
			if(timestamp[1].equals("04")) {
				itemID.set(tokens[2]);
				context.write(itemID, one);
			}
		}
	}
	
	public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text itemID, Iterable<IntWritable> ones, Context context) throws IOException, InterruptedException {
			int count = 0;
			Iterator<IntWritable> iterator = ones.iterator();
			while(iterator.hasNext()) {
				count++;
				iterator.next();
			}
			
			IntWritable clicks = new IntWritable(count);
			context.write(itemID, clicks);
		}
	}
	
	public static class ItemClickMapper2 extends Mapper<Object, Text, IntWritable, Text>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			Text itemId = new Text(tokens[0]);
			int clicks = Integer.parseInt(tokens[1]);
			IntWritable clicksFinal = new IntWritable(clicks);
			context.write(clicksFinal, itemId);
		}
	}
	
	public static class SumReducer2 extends Reducer<IntWritable, Text, Text, IntWritable>{
		
		private Map<String, Integer> countMap = new HashMap<String, Integer>();
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text value: values) {
				countMap.put(value.toString(), key.get());
			}
			
	    }
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			Map<String, Integer> sortedMap = sort(countMap);
	  		  int counter = 0;
	  		  for (String key: sortedMap.keySet()) {
	  			  if (counter ++ == 10) {
	  				  break;
	  			  }
	  			  context.write(new Text(key),new IntWritable(sortedMap.get(key)));
	  		  }
		}
		
		public static Map<String, Integer> sort(Map<String, Integer> unsortMap) {

	        List<Map.Entry<String, Integer>> list =
	                new LinkedList<Map.Entry<String, Integer>>(unsortMap.entrySet());
//	        for (Map.Entry<String, Integer> entry : list) {
//	            System.out.println("-----IN COMPARATOR METHOD------"+entry.getKey()+"  "+entry.getValue());
//	        }
	        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
	            public int compare(Map.Entry<String, Integer> o1,
	                               Map.Entry<String, Integer> o2) {
	            	if(!o2.getValue().equals(o1.getValue()))
	            		return (o2.getValue()).compareTo(o1.getValue());
	            	else
	            		return (o1.getKey().compareTo(o2.getKey()));
	            }
	        });

	        Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
	        for (Map.Entry<String, Integer> entry : list) {
	            sortedMap.put(entry.getKey(), entry.getValue());
	        }

	        return sortedMap;
	    }
		
			//context.write(itemId, key);
    }
	
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf1 = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		
		if (otherArgs.length !=2) {
			System.err.println("Usage: input_file output_file");
			System.exit(2);
		}
		
		Job job1 = Job.getInstance(conf1, "Item Click");
		job1.setJarByClass(ItemClick.class);
		job1.setMapperClass(ItemClickMapper.class);
		job1.setReducerClass(SumReducer.class);
		job1.setNumReduceTasks(10);
		job1.setOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]+"/temp"));
		
		if (!job1.waitForCompletion(true)) {
			  System.exit(1);
		}
		
		Job job2 = Job.getInstance(conf1);
	    job2.setJarByClass(ItemClick.class);
	    job2.setJobName("sort");
	    job2.setNumReduceTasks(1);

	    FileInputFormat.setInputPaths(job2, new Path(otherArgs[1] + "/temp"));
	    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1] + "/final"));

	    job2.setMapperClass(ItemClickMapper2.class);
	    job2.setReducerClass(SumReducer2.class);

	    job2.setMapOutputKeyClass(IntWritable.class);
	    job2.setMapOutputValueClass(Text.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(IntWritable.class);
	    
	    if (!job2.waitForCompletion(true)) {
			  System.exit(1);
		}
	}
}
