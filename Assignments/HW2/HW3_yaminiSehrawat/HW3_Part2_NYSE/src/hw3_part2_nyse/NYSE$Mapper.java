/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part2_nyse;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author yamini
 */
public class NYSE$Mapper extends Mapper<Object, Text, Text, DoubleWritable>{
    
   private final static DoubleWritable stockPriceHigh = new DoubleWritable();
  
 
  @Override
  protected void map(Object key, Text value, Mapper<Object,Text,Text,DoubleWritable>.Context context)
          throws IOException, InterruptedException
  {
   String[] tokens = value.toString().split(",");
   try{
       Text stock = new Text();
       stock.set(tokens[1]);
       stockPriceHigh.set(Double.parseDouble(tokens[4]));
       context.write(stock,stockPriceHigh);
   }
   catch(Exception e){
       System.out.println("Exception caught:"+e.getMessage());
   }
   
  }
}
