/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part4;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author yamini
 */
public class HW3$Reducer extends Reducer<Text, StockStatsInfo, Text, StockStatsInfo>
{
    private StockStatsInfo result = new StockStatsInfo();
    
    @Override
    public void reduce(Text key, Iterable<StockStatsInfo> values, Context context)
            throws IOException, InterruptedException
    {
        Integer minStockVolume = 0;
        Integer maxStockVolume = 0;
        
        result.setMinStockVolume(null);
        result.setMaxStockVolume(null);
        
        for(StockStatsInfo val:values){
             minStockVolume = val.getMinStockVolume();
             maxStockVolume = val.getMaxStockVolume();
             
             if(result.getMinStockVolume() == null || minStockVolume.compareTo(result.getMinStockVolume())<0)
             {
                 result.setMinStockVolume(minStockVolume);
             }
             if(result.getMaxStockVolume() == null || maxStockVolume.compareTo(result.getMaxStockVolume())>0)
             {
                 result.setMaxStockVolume(maxStockVolume);
             }
        }
        context.write(key, result);
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
     
     /* double min = Double.MAX_VALUE;
      double max = Double.MIN_VALUE;
        for(StockStatsInfo value:values){
            
            min = Math.min(min, value.getMinStockVolume());
            result.setMinStockVolume(min);
            
            max = Math.max(max, value.getMaxStockVolume());
            result.setMaxStockVolume(max);
            
            if ((this.result.getMinStockVolume()== 0) || (value.getMinStockVolume() < this.result.getMinStockVolume())) 
            {
                this.result.setMinStockVolume(value.getMinStockVolume());
            }
            if ((this.result.getMaxStockVolume() == 0) || (value.getMaxStockVolume() > this.result.getMaxStockVolume())) 
            {
                this.result.setMaxStockVolume(value.getMaxStockVolume());
            }
            
        }
        
        try
        {
           context.write(key, this.result);
        } 
        catch (Exception ex)
        {
          System.out.println("Exception"+ex.getMessage());
        }*/
        
    }
    
}
