/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part2_nyse;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author yamini
 */
public class HW3_Part2_NYSE {
    
    public static void main(String[] args) 
            throws IOException, InterruptedException, ClassNotFoundException{
        
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1,"MovieLens_HW2Part4");
        FileSystem hdfs = FileSystem.get(conf1);
        FileSystem local = FileSystem.getLocal(conf1);
        
        Path inputDir = new Path(args[0]);
        Path hdfsFile = new Path(args[1]);
        
        try{
            FileStatus[] inputFiles = local.listStatus(inputDir);
            FSDataOutputStream out = hdfs.create(hdfsFile);
            
            for(int i=0; i<inputFiles.length; i++){
                FSDataInputStream in = local.open(inputFiles[i].getPath());
                byte[] buffer = new byte[256];
                int bytesRead = 0;
                
                while((bytesRead = in.read(buffer))>0){
                    out.write(buffer, 0, bytesRead);
                }
                in.close();
                
            }
            out.close();
            
        }catch(IOException ex){
            ex.printStackTrace();
        }
        
       
      //  boolean complete = job1.waitForCompletion(true);
        
        
       // Configuration conf2 = new Configuration();
      // Job job2 = Job.getInstance(conf2,"MovieLensHW2Part4");
      // if(complete){
        job1.setJarByClass(HW3_Part2_NYSE.class);
        job1.setMapperClass(NYSE$Mapper.class);
        job1.setCombinerClass(NYSE$Reducer.class);
        job1.setReducerClass(NYSE$Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
     //  }
        
    }
    
}
