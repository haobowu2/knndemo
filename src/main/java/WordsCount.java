import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import java.util.Iterator;
import java.io.IOException;


public class WordsCount {
    public static class myMap extends Mapper<LongWritable,Text,Text,IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            String[] data = value.toString().split("[ ]");

            for (int i = 0; i < data.length; i++)
                if (!data[i].equals(""))
                    context.write(new Text(data[i]), new IntWritable(1));
        }
    }
    public static class myReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key,Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
            Iterator<IntWritable>iterator = values.iterator();
            int sum = 0;
            while (iterator.hasNext()){
                IntWritable next = iterator.next();
                sum += next.get();
            }
            context.write(key,new IntWritable(sum));

        }
    }

    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{

        Path input = new Path("D://words.txt");
        Path output = new Path("wordsCount.txt");

        //创建conf对象
        Configuration conf = new Configuration();


        //获取job对象
        Job job = Job.getInstance(conf,"WordsCount");
        //设置jar
        job.setJarByClass(WordsCount.class);
        //设置map和reduce
        job.setMapperClass(myMap.class);
        job.setReducerClass(myReduce.class);
        //设置输出key value 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置输出输入路径
        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);
        //提交job

        boolean b = job.waitForCompletion(true);

        System.exit(b? 0:1);

    }
}
