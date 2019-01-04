package com.briup.chrasm;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import javax.imageio.ImageIO;

import org.apache.hadoop.io.Text;
import java.awt.image.BufferedImage;
import java.io.IOException;


public class ImageRecordReader extends RecordReader {

    private Text key;
    private ArrayWritable value;

    private boolean flag = true;



    //数据分片
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit data = (FileSplit) inputSplit;
        Path path = data.getPath();
        key = new Text(path.getName());


        FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
        BufferedImage image = ImageIO.read(fs.open(path));
        int width = image.getWidth();
        int height = image.getHeight();
        IntWritable[] pix = new IntWritable[width * height];
        for (int x = 0; x < height; x++)
            for (int y = 0; y < width; y++)
                pix[x * width + y] = new IntWritable(image.getRGB(x, y));
        value = new ArrayWritable(IntWritable.class);
        value.set(pix);
        fs.close();

    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean f = flag;
        flag = false;
        return f;
    }

    public Object getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public Object getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    public void close() throws IOException {

    }
}

