/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part3_keyvalueinfmt;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author yamini
 */
public class KeyValue$Reducer extends Reducer<Text, Text, Text, Text>
{
   private Text result = new Text();
 
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException 
    {
        String translations = "";
 
        for (Text val : values) {
            translations += "|" + val.toString();
        }
 
        result.set(translations);
        context.write(key, result);
    }
    
}
