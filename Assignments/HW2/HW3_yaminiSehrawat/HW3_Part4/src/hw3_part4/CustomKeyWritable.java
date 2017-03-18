/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 * @author yamini
 */
public class CustomKeyWritable implements Writable, WritableComparable<CustomKeyWritable>{
    
    
    private String stockSymbol;
    private String stockDate;
    
    
    public CustomKeyWritable()
   {
       
   }
    
    public CustomKeyWritable(String s, String d)
   {
     this.stockSymbol = s;
     this.stockDate = d;
   }
    
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd'T'HH:mm:ss.SSS");
    
    @Override
    public int compareTo(CustomKeyWritable o) {
         int result = this.stockSymbol.compareTo(o.stockSymbol);
       
       if(result==0)
       {
          result = this.stockDate.compareTo(o.stockDate);
       }
       
       return result;
    }
    

    @Override
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeString(out, this.stockSymbol);
      WritableUtils.writeString(out, this.stockDate);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
       this.stockSymbol = WritableUtils.readString(in);
       this.stockDate = WritableUtils.readString(in);
    }

   

    public String getStockSymbol() {
        return stockSymbol;
    }

    public void setStockSymbol(String stockSymbol) {
        this.stockSymbol = stockSymbol;
    }

    public String getStockDate() {
        return stockDate;
    }

    public void setStockDate(String stockDate) {
        this.stockDate = stockDate;
    }

    
    @Override
    public String toString() {
    return this.stockSymbol + " " +this.stockDate;

    }

   
    
}
