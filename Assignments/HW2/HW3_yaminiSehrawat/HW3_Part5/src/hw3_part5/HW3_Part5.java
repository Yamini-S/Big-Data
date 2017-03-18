/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part5;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author yamini
 */
public class HW3_Part5 {

            

    
    public static class HW3$Mapper1 extends Mapper<LongWritable, Text, Text, DoubleWritable>
    {
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] currentRatingsTuple = line.split("::");
			context.write(new Text(currentRatingsTuple[1]), new DoubleWritable(Double.valueOf(currentRatingsTuple[2])));
		}
    }
    
    public static class HW3$Reducer1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
       @Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
		{
			double sum=0, count= 0;
                        
			for(DoubleWritable value : values)
			{
				sum+=value.get();
				count++;
			}
			double avg = (double)sum/count;
			context.write(key, new DoubleWritable(avg));
		}
        
    }
    
    
    //Custom Comparator for comparison of double values
	public static class DoubleComparator extends WritableComparator 
	{
		public DoubleComparator() 
		{
			super(DoubleWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) 
		{
			Double firstValue = ByteBuffer.wrap(b1, s1, l1).getDouble();
			Double secondValue = ByteBuffer.wrap(b2, s2, l2).getDouble();
			return firstValue.compareTo(secondValue) * (-1);
		}
	}
    
        
        
        
        //Mapper2 and Reducer2 for sorting and extracting top 25 highes rated movies.
    
    public static class HW3$Mapper2 extends Mapper<Object, Text, DoubleWritable, Text>
    {
        private HashMap<String, Double> topTwentyFive = new HashMap<String, Double>();
        public int counter = 0;
        
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
                
        {
            String[] line = value.toString().split("\t");
            Double rating = Double.parseDouble(line[1]);
            
            Integer totalCount = 25;
            
            if(counter<totalCount)
            {
                topTwentyFive.put(line[0], rating);
		counter++;
            }
            else{
                String lowerKey = null;
                Double min = 10.0; //Initializing with 10(any number greater than 5) as max value of rating is 5 
                
                for(String currentKey:topTwentyFive.keySet())
                {
                    if(topTwentyFive.get(currentKey)<min){
                        min = topTwentyFive.get(currentKey);
                        lowerKey = currentKey;
                    }
                    
                }
                
                if(rating>min)
                {
                   topTwentyFive.remove(lowerKey);
                   topTwentyFive.put(line[0], rating);
                }
            }
			
            
        }
        
        @Override
		protected void cleanup(Context context)throws IOException, InterruptedException
		{
			for(String currentKey:topTwentyFive.keySet())
			{
				context.write( new DoubleWritable(topTwentyFive.get(currentKey)),new Text(currentKey));
			}
		}
    
    }
    
    
    public static class HW3$Reducer2 extends Reducer<DoubleWritable, Text, Text, DoubleWritable>
    {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException 
        {
         System.out.println("reducer2started");
         
         for(Text value: values){
             context.write(value,key);         
         
        }
        
    }
    
}
    
    
    public static void main(String[] args) throws Exception {
    
         Configuration conf1 = new Configuration();
         Job job1 = Job.getInstance(conf1,"rating movies");
         job1.setJarByClass(HW3_Part5.class);
         job1.setMapperClass(HW3$Mapper1.class);
         
         job1.setReducerClass(HW3$Reducer1.class);
         job1.setOutputKeyClass(Text.class);
         job1.setOutputValueClass(DoubleWritable.class);
         job1.setInputFormatClass(TextInputFormat.class);
	 job1.setOutputFormatClass(TextOutputFormat.class);
         
         FileInputFormat.addInputPath(job1,new Path(args[0]));
         FileOutputFormat.setOutputPath(job1,new Path(args[1]));
         
        boolean complete = job1.waitForCompletion(true);
         
         
         
        
         Configuration conf2 = new Configuration();
         Job job2 = Job.getInstance(conf2,"sort");
         
         if(complete){
         job2.setJarByClass(HW3_Part5.class);
         job2.setMapperClass(HW3$Mapper2.class);         
         job2.setReducerClass(HW3$Reducer2.class);
         job2.setOutputKeyClass(DoubleWritable.class);
         job2.setOutputValueClass(Text.class);
         job2.setSortComparatorClass(DoubleComparator.class);
         job2.setOutputFormatClass(TextOutputFormat.class);
         //job2.setComparator(Writable COmparator);
         
         FileInputFormat.addInputPath(job2,new Path(args[1]));
         FileOutputFormat.setOutputPath(job2,new Path(args[2]));
         System.exit(job2.waitForCompletion(true)?0:1);
         }
   
    }
}
