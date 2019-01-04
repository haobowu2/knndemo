package com.briup.chrasm;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;


public class ImageInputFormat extends FileInputFormat {


    protected boolean isSplitable(JobContext context, Path filename){
        return false;
    }

    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new ImageRecordReader();
    }
}

