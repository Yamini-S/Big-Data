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
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 *
 * @author yamini
 */
public class CombineEmployeeInFmt extends CombineFileInputFormat<LongWritable, Text>
{

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) 
            throws IOException 
    {
        CombineFileRecordReader<LongWritable,Text> reader = new CombineFileRecordReader<LongWritable,Text>((CombineFileSplit)split, context, EmpCombineFileRecordReader.class);
            return reader;
    }
    
    
}


