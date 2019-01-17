import net.toalaska.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/*
输出源于reducer任务
如果要得到多个输出文件 ，需要有同样数据的reducer在运行
reducer任务源于mapper的数据划分（Partition）
 */
public class MyPartitiner {

    private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           // System.out.printf("key : %s\n",key);
            //System.out.printf("value : %s\n",value);
                    String[] s=value.toString().split(" ");
                   // System.out.printf("s : %s\n",s.length);

                    /*
                    * mobiles.txt
                    xiaomi\t1
                    huawei\t2
                    * */
                    context.write(new Text(s[0]),new IntWritable(Integer.parseInt(s[1])));
        }


    }
    private static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum=0;
            for (IntWritable value : values) {
                sum+=value.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }
    private static class MyPartitioner extends Partitioner<Text,IntWritable>{
        @Override
        public int getPartition(Text key, IntWritable intWritable, int i) {
            System.out.printf("getPartition:%s -> %s\n",key.toString(),intWritable);
            if(key.toString().equals("xiaomi")){
                return 0;
            }
            if(key.toString().equals("huawei")){
                return 1;
            }
            if(key.toString().equals("iphone")){
                return 2;
            }
            return 3;
        }
    }


    private Configuration configuration;
    private FileSystem fileSystem;
    private Path input;
    private Path output;

    @Before
    public void setUp() throws URISyntaxException, IOException {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);

//

        input = new Path("hdfs://localhost:9000/input/mobiles.txt");
        output = new Path("hdfs://localhost:9000/haha/mobiles_out");



        if(fileSystem.exists(output)){
            fileSystem.delete(output,true);

        }

    }

    @Test
    public void run() throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance(configuration,"MyPartitiner");
        job.setJarByClass(MyPartitiner.class);
        //map
        job.setMapperClass(MyPartitiner.MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //reduce
        job.setReducerClass(MyPartitiner.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置 partitioner
        job.setPartitionerClass(MyPartitioner.class);
        //设置 4个分区 !!!!
        job.setNumReduceTasks(4);
        //设置输入格式
        job.setInputFormatClass(TextInputFormat.class);
        Path inputPath=input;
        FileInputFormat.addInputPath(job,inputPath);
        //设置输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,output);
        //
        boolean isok=job.waitForCompletion(true);
        System.out.println(isok);


        /*
        hadoop fs -ls /haha/mobiles_out
        hadoop fs -cat /haha/mobiles_out/part-r-00000
        hadoop fs -cat /haha/mobiles_out/part-r-00001
        hadoop fs -cat /haha/mobiles_out/part-r-00002
        hadoop fs -cat /haha/mobiles_out/part-r-00003

*/


    }

}
