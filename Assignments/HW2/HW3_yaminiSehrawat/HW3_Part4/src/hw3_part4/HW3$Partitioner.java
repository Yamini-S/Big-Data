/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part4;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author yamini
 */
public class HW3$Partitioner extends Partitioner<CustomKeyWritable, StockStatsInfo>
{
    public int getPartition(CustomKeyWritable key,StockStatsInfo value, int numOfPartitions){
        return key.getStockSymbol().hashCode() % numOfPartitions;
        
    }
    
}
