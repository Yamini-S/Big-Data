/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part4;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author yamini
 */
public class HW3$Mapper extends Mapper<Object,Text, Text,StockStatsInfo> {
    
    private Text date = new Text();
   // private Integer minStockVolume;
    //private Integer maxStockVolume;
    
    public StockStatsInfo output = new StockStatsInfo();
    
    public void map(Object Key, Text value, Context context)
            throws IOException, InterruptedException
    {
        
        
        try
    {
     
        String[] values = value.toString().split(",");
        date.set(values[2]);
        output.setMinStockVolume(Integer.parseInt(values[7]));
        output.setMaxStockVolume(Integer.parseInt(values[7]));
        
        context.write(date, output);
      
    }
    catch (Exception e)
    {
      System.out.println(e);
    }
        
        
        
        
        
        
        
           /*     String[] values = value.toString().split(",");
                date.set(values[2]);
                minStockVolume = Integer.parseInt(values[7]);
                maxStockVolume = Integer.parseInt(values[7]);
                
                if(date==null || minStockVolume == 0 || maxStockVolume == 0){
                    return;
                }
                
                try{
                    output.setMinStockVolume(minStockVolume);
                    output.setMaxStockVolume(maxStockVolume);
                    context.write(date,output);
                         
                }catch(Exception ex){
                    ex.printStackTrace();
                }*/
                        
    }
    
}
