package com.briup.chrasm;

;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;

public class Grayscable {
    public static class myMap extends Mapper<Text,ArrayWritable,Text,Text>{

        @Override
        protected void map (Text key,ArrayWritable value,Context context) throws IOException,InterruptedException{
            IntWritable[] writables = (IntWritable[]) value.get();

            for (int i = 0; i < writables.length; i++) {
                int pix = writables[i].get();
                int gray = (pix >> 16 & 0xFF) + (pix >> 8 & 0xFF) + (pix & 0xFF);
                if (gray > 255) writables[i].set(1);
                else writables[i].set(0);
            }
            String v = Arrays.toString(writables).replace(" "," ");
            context.write(key, new Text(v.substring(1,v.length()-1) ));
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        Path input = new Path("data/knn/knn_train");
        input = new Path("data/0_1.png");
        Path output = new Path("data/knn/knn_grayscable_train");
        output = new Path("data/rr");

        Job job = Job.getInstance(new Configuration(), "Grayscable");

        job.setJarByClass(Grayscable.class);
        job.setMapperClass(Grayscable.myMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(ImageInputFormat.class);

        ImageInputFormat.addInputPath(job,input);
        TextOutputFormat.setOutputPath(job,output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
