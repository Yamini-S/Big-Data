/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part2_nyse;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author yamini
 */
public class NYSE$Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
    
    public DoubleWritable result = new DoubleWritable();
    
    public void reduce(Text key, Iterable<DoubleWritable> values,Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
            throws IOException, InterruptedException
    {
        double sum = 0.0D;
        double avgStockPrice = 0.0D;
        
        int count = 0;
        
        for(DoubleWritable val :values){
            sum+=val.get();
            count++;
        }
        
        avgStockPrice = sum/count;
        result.set(avgStockPrice);
        context.write(key,result);
    }
    
}
