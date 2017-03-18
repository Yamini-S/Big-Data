/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part3_nlineinfmt;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author yamini
 */
public class NLineInFmt$Reducer extends Reducer<Text, Text, Text, Text>
{
        @Override
        public void reduce(Text key, Iterable<Text> value, Context context) 
                throws IOException, InterruptedException
        {
            for (Text values : value){
                context.write(key, values);
            }
        }
    
}
