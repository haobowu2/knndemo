package com.briup.chrasm;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.testng.annotations.Test;

import java.io.IOException;


public class ImageInputformatTest  {
    public static class mymap extends Mapper<Text,ArrayWritable,NullWritable,NullWritable> {
        protected void map(Text key, ArrayWritable value, Context context) throws IOException, InterruptedException {
            IntWritable[] writables = (IntWritable[]) value.get();

            for (int i = 0; i < writables.length; i++) {
                int pix = writables[i].get();
                int gray = (pix >> 16 & 0xFF) + (pix >> 8 & 0xFF) + (pix & 0xFF);
                if (gray > 255) writables[i].set(1);
                else writables[i].set(0);
            }
            System.out.println(this);
            for (int i = 0; i < 20; i++) {
                for (int j = 0; j < 20; j++) {
                    System.out.print(writables[i * 20 + j]);
                }
                System.out.println();
            }

        }
    }


        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Path input = new Path("data/knn");

            Job job = Job.getInstance(new Configuration(), "ImageInputformatTest");
            job.setJarByClass(Test.class);
            job.setMapperClass(mymap.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(NullWritable.class);

            job.setInputFormatClass(ImageInputFormat.class);

            ImageInputFormat.addInputPath(job, input);
            TextOutputFormat.setOutputPath(job, new Path("data/knnResult"));

            System.exit(job.waitForCompletion(true) ? 0 : 1);


        }
    }