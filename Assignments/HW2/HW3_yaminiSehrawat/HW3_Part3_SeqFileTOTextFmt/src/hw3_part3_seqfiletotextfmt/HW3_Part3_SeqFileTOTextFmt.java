/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part3_seqfiletotextfmt;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author yamini
 */
public class HW3_Part3_SeqFileTOTextFmt {

    public static void main(String[] args) 
            throws IOException, InterruptedException, ClassNotFoundException
    {
        try{
        Configuration conf = new Configuration();
        //conf.set("key.value.separator.in.input.line", ","); 
        Job job = Job.getInstance(conf,"read sequence file");
        job.setJarByClass(HW3_Part3_SeqFileTOTextFmt.class);

        job.setMapperClass(SeqToTextFmt$Mapper.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
        }
        catch(Exception ex){
            System.out.println("Exception"+ex.getMessage());
        }
    }
        
        
}
    

