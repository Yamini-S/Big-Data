/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part4;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author yamini
 */
public class HW3_Part4 {
    public static void main(String[] args) 
            throws IOException, InterruptedException, ClassNotFoundException
    {
        
         
       try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf,"NYSE Stats");
            job.setJarByClass(HW3_Part4.class);
            job.setMapperClass(HW3$Mapper.class);
            job.setCombinerClass(HW3$Reducer.class);
            job.setReducerClass(HW3$Reducer.class);
            
           // job.setMapOutputKeyClass(CustomKeyWritable.class);
           // job.setMapOutputValueClass(StockStatsInfo.class);
           // job.setPartitionerClass(HW3$Partitioner.class);
           // job.setGroupingComparatorClass(GroupingComparator.class);
            
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(StockStatsInfo.class);        
            
            FileInputFormat.addInputPath(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));
            System.exit(job.waitForCompletion(true)?0:1);
        } catch (IOException ex) {
            Logger.getLogger(HW3_Part4.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
