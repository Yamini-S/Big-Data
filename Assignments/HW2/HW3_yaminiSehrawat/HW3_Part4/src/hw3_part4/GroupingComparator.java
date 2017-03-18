/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author yamini
 */
public class GroupingComparator extends WritableComparator{
    
    protected GroupingComparator(){
        super(CustomKeyWritable.class, true);
    }
    
    public int compare(WritableComparable w1, WritableComparable w2)
  {
    CustomKeyWritable cw1 = (CustomKeyWritable)w1;
    CustomKeyWritable cw2 = (CustomKeyWritable)w2;
    return cw1.getStockDate().compareTo(cw2.getStockDate());
  }
    
}
