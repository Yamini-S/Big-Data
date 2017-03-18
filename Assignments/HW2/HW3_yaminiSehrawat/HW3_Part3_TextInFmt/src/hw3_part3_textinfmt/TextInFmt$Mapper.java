/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part3_textinfmt;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author yamini
 */
public class TextInFmt$Mapper extends Mapper<Object,Text,Text,IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    @Override
    protected void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException{
        try{
        String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line);
        
        while(itr.hasMoreTokens()){
            word.set(itr.nextToken());
            context.write(word,one);
        }
        }catch(Exception ex){
            System.out.println("Exception"+ ex.getMessage());
        }
        
    }
    
}
