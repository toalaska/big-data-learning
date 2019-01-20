package demo.traffice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;

public class TrafficApp {

    // mapper class
    public static class MyMapper extends Mapper<LongWritable, Text, Text, TrafficWritable> {

        private Text outputKey = new Text();
        private TrafficWritable outputValue = new TrafficWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String lineValue = value.toString();

            String[] strs = lineValue.split("\\|");
            System.out.printf("strs:%s\n",strs.length);

            String phoneNum = strs[0];
            int upPackNum = Integer.valueOf(strs[1]);
            int downPackNum = Integer.valueOf(strs[2]);
            int upPayLoad = Integer.valueOf(strs[3]);
            int downPayLoad = Integer.valueOf(strs[4]);

            // set
            outputKey.set(phoneNum);
            outputValue.set(upPackNum, downPackNum, upPayLoad, downPayLoad);

            // context write
            context.write(outputKey, outputValue);
        }
    }

    // reducer class
    public static class MyReducer extends Reducer<Text, TrafficWritable, Text, TrafficWritable> {

        private TrafficWritable outputValue = new TrafficWritable();

        @Override
        protected void reduce(Text key, Iterable<TrafficWritable> v2s,
                              Context context) throws IOException, InterruptedException {
            int upPackNum = 0;
            int downPackNum = 0;
            int upPayLoad = 0;
            int downPayLoad = 0;

            for (TrafficWritable value : v2s) {
                upPackNum += value.getUpPackNum();
                downPackNum += value.getDownPackNum();
                upPayLoad += value.getUpPayLoad();
                downPayLoad += value.getDownPayLoad();
            }

            // set
            outputValue.set(upPackNum, downPackNum, upPayLoad, downPayLoad);

            // context write
            context.write(key, outputValue);
        }
    }

    // driver
    public static void main(String[] args) throws Exception {

        String INPUT_PATH = "hdfs://localhost:9000/input/traffice.txt";
        String OUTPUT_PATH = "hdfs://localhost:9000/haha/outputtraffic";

        Configuration conf = new Configuration();
        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
        if (fileSystem.exists(new Path(OUTPUT_PATH))) {
            fileSystem.delete(new Path(OUTPUT_PATH), true);
        }

        Job job = Job.getInstance(conf, "TrafficApp");

        // run jar class
        job.setJarByClass(TrafficApp.class);

        // 设置map
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TrafficWritable.class);

        // 设置reduce
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TrafficWritable.class);

        // input formart
        job.setInputFormatClass(TextInputFormat.class);
        Path inputPath = new Path(INPUT_PATH);
        FileInputFormat.addInputPath(job, inputPath);

        // output format
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path(OUTPUT_PATH);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 提交job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

