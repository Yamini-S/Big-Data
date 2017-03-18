/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part3_textinfmt;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author yamini
 */
public class HW3_Part3_TextInFmt {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args)
             throws IOException, InterruptedException, ClassNotFoundException
    {
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"TextFormatInput");
        job.setJarByClass(HW3_Part3_TextInFmt.class);
        job.setMapperClass(TextInFmt$Mapper.class);
        job.setCombinerClass(TextInFmt$Reducer.class);
        job.setReducerClass(TextInFmt$Reducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
        
    }
    
}
   
