/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part3_seqfiletotextfmt;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author yamini
 */
public class SeqToTextFmt$Mapper extends Mapper <Text, Text, Text, Text>{
    
     @Override
     protected void map(Text key, Text value,Mapper.Context context) 
             throws IOException, InterruptedException 
     {
            context.write(key, value);               
     }
}
    

