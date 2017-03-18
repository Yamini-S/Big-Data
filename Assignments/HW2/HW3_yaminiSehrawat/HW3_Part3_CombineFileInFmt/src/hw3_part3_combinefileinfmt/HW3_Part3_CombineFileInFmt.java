/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part3_combinefileinfmt;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 *
 * @author yamini
 */
public class HW3_Part3_CombineFileInFmt {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        try{
        Configuration conf = new Configuration();
       // conf.set("mapred.max.split.size", "1048576"); //1MB
        
        Job job = Job.getInstance(conf,"combine small files");
        job.setJarByClass(HW3_Part3_CombineFileInFmt.class);

        job.setMapperClass(CombineFileInFmt$Mapper.class);
        job.setReducerClass(CombineFileInFmt$Reducer.class);
        job.setInputFormatClass(CombineEmployeeInFmt.class);
        
        CombineEmployeeInFmt.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        System.exit(job.waitForCompletion(true)?0:1);
        }
        catch(Exception ex){
            System.out.println("Exception"+ex.getMessage());
        }
    }
    
}
