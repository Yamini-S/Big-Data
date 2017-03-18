/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hw3_part3_combinefileinfmt;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 *
 * @author yamini
 */
public class EmpCombineFileRecordReader extends RecordReader<LongWritable,Text>{
        private LineRecordReader lineRecordReader = new LineRecordReader();
        
        public EmpCombineFileRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) 
            throws IOException{
            FileSplit fileSplit = new FileSplit(split.getPath(index),split.getOffset(index),
                    split.getLength(index), split.getLocations());
            lineRecordReader.initialize(fileSplit, context);
        }
        
        
        @Override
        public void initialize(InputSplit split,TaskAttemptContext context)
            throws IOException, InterruptedException{
            //
        }
        
        @Override
        public void close() throws IOException{
            lineRecordReader.close();
        }
        
        @Override
        public float getProgress() throws IOException{
            return lineRecordReader.getProgress();
        }
        
        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException{
            return lineRecordReader.getCurrentKey();
        }
        
        @Override
        public Text getCurrentValue() throws IOException,InterruptedException{
            return lineRecordReader.getCurrentValue();
        }
        
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException{
            return lineRecordReader.nextKeyValue();
        }
}