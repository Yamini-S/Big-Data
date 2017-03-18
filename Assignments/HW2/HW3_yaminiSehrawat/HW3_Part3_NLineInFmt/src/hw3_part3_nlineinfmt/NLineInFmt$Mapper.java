/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part3_nlineinfmt;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author yamini
 */
public class NLineInFmt$Mapper extends Mapper<LongWritable, Text, Text, Text>{
    
        @Override
        public void map(LongWritable key, Text value,Context context) 
                throws IOException, InterruptedException
        {
            String[] tokens = value.toString().split("\t");
            Text txtKey = new Text("");
            Text txtValue = new Text("");
            
            txtKey.set(tokens[0]);
            txtValue.set(tokens[2] + "\t" + tokens[3]);
            context.write(txtKey,txtValue);
        }
    
}
