package com.briup.chrasm;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
public class KNN {
    public static class myMap extends Mapper<LongWritable,Text,Text,Text>{
        private int K;
        private Path trainPath;
        @Override
        protected void setup(Context context) throws IOException,InterruptedException{
            K = Integer.parseInt(context.getConfiguration().get("knn.k","10"));
            trainPath = new Path(context.getConfiguration().get("knn.train.path"));
        }
        @Override
        protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
            TreeMap<Double, String> k_rows = new TreeMap<>(new Comparator<Double>() {
                @Override
                public int compare(Double o1, Double o2) {
                    int r = o2.compareTo(o1);
                    return r == 0 ? 1 : r;
                }
            });
            String[] test = value.toString().split("[\t]");
            String[] test_pix = test[1].split("[,]");

            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(fs.open(trainPath)));
            String line = "";
            while ((line = br.readLine()) != null) {
                String[] train = line.split("[\t]");
                String[] train_pix = train[1].split("[,]");
                if (test_pix.length != train_pix.length) continue;
                double sim = 0;
                for (int i = 0; i < test_pix.length; i++) {
                    int d = Integer.parseInt(train_pix[i]) - Integer.parseInt(test_pix[i]);
                    sim += d * d;
                }
                sim = 1 / (Math.sqrt(sim) + 1);
                k_rows.put(sim, train[0]);
                if (k_rows.size() > K) k_rows.pollLastEntry();
            }
            Iterator<Map.Entry<Double, String>> iterator = k_rows.entrySet().iterator();
            String tag = UUID.randomUUID().toString();
            while (iterator.hasNext()) {
                Map.Entry<Double, String> next = iterator.next();
                context.write(new Text(test[0] + ":" + tag), new Text(next.getValue() + ":" + next.getKey()));
            }
        }
    }
    public static class myReduce extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> count = new HashMap<>();
            Map<String, Double> sumSim = new HashMap<>();
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                String[] row = iterator.next().toString().split("[:]");
                String number = row[0].split("[_]")[0];
                double sim = Double.parseDouble(row[1]);
                count.put(number, count.getOrDefault(number, 0) + 1);
                sumSim.put(number, sumSim.getOrDefault(number, .0) + sim);
            }
            StringBuffer v = new StringBuffer();
            Iterator<Map.Entry<String, Integer>> iter = count.entrySet().iterator();
            Map.Entry<String, Integer> result = iter.next();
            v.append(result.getKey())
                    .append("[")
                    .append("avgSim: ")
                    .append(sumSim.get(result.getKey()) / result.getValue())
                    .append(", count: ")
                    .append(result.getValue())
                    .append("], ");
            while (iter.hasNext()) {
                Map.Entry<String, Integer> next = iter.next();
                v.append(next.getKey())
                        .append("[")
                        .append("avgSim: ")
                        .append(sumSim.get(next.getKey()) / next.getValue())
                        .append(", count: ")
                        .append(next.getValue())
                        .append("], ");
                if (next.getValue() > result.getValue() ||
                        (next.getValue() == result.getValue() &&
                                sumSim.get(result.getKey()) / result.getValue() < sumSim.get(next.getKey()) / next.getValue())
                        ) result = next;
            }
            context.write(new Text(key.toString().split("[:]")[0] + ", result: " + result.getKey()), new Text(v.toString()));
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String trainPath = "data/knn/knn_grayscable_train/part-r-00000";
        int K = 10;
        Path input = new Path("data/knn/knn_grayscable_test/part-r-00000");
        Path output = new Path("data/knn/knn_result_"+K);
        Job job = Job.getInstance(new Configuration(), "KNN");
        Configuration conf = job.getConfiguration();
        conf.set("knn.train.path", trainPath);
        conf.set("knn.K", ""+K);
        job.setJarByClass(KNN.class);
        job.setMapperClass(KNN.myMap.class);
        job.setReducerClass(KNN.myReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job, input);
        TextOutputFormat.setOutputPath(job, output);
        System.exit( job.waitForCompletion(true) ? 0:1 );
    }
}
