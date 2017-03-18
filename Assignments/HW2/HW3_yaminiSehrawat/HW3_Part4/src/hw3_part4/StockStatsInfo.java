/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author yamini
 */
public class StockStatsInfo implements Writable 
{
  
    Integer minStockVolume;
    Integer maxStockVolume;
  
  public StockStatsInfo() 
  {
      minStockVolume = 0;
      minStockVolume = 0;
  }
  
  public StockStatsInfo(Integer min, Integer max)
  {
    this.minStockVolume = min;
    this.maxStockVolume = max;
  }
  
  public Integer getMinStockVolume()
  {
    return this.minStockVolume;
  }
  
  public void setMinStockVolume(Integer minStockVolume)
  {
    this.minStockVolume = minStockVolume;
  }
  
  public Integer getMaxStockVolume()
  {
    return this.maxStockVolume;
  }
  
  public void setMaxStockVolume(Integer maxStockVolume)
  {
    this.maxStockVolume = maxStockVolume;
  }
  
  public void write(DataOutput out)
    throws IOException
  {
    out.writeInt(this.minStockVolume);
    out.writeInt(this.maxStockVolume);
  }
  
  public void readFields(DataInput in)
    throws IOException
  {
    this.minStockVolume = new Integer(in.readInt());
    this.maxStockVolume = new Integer(in.readInt());
  }
  
  public String toString()
  {
    return this.minStockVolume + "\t" + this.maxStockVolume;
  }
    
}
