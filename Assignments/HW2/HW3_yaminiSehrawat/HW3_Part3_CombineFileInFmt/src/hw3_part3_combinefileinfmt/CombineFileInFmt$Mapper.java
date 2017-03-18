/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part3_combinefileinfmt;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author yamini
 */
public class CombineFileInFmt$Mapper extends Mapper<LongWritable, Text, Text, Text>
{
    
	Text txtKey = new Text("");
	Text txtValue = new Text("");

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException 
        {
            
            String[] tokens = value.toString().split("\\t");
            txtKey.set(tokens[0]);
            txtValue.set(tokens[2] + "\t" + tokens[3]);
            context.write(txtKey,txtValue);
        }
}
