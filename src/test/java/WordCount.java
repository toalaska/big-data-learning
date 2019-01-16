import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet2.HamletSpec;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

public class WordCount {

    private Configuration configuration;
    private FileSystem fileSystem;
    private Path input;
    private Path output;

    @Before
    public void setUp() throws URISyntaxException, IOException {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);

//       这里的path要以hdfs://localhost:9000  开头
        input = new Path("hdfs://localhost:9000/input/a.txt");
        output = new Path("hdfs://localhost:9000/haha/word_count_out");

        if(fileSystem.exists(output)){
            fileSystem.delete(output,true);

        }


    }

    public static class WordCountApp{
        public static class MyMapper extends Mapper<Object,Text, Text, IntWritable> {
            IntWritable intWritable=new IntWritable(1);
            Text word=new Text();
            @Override
            public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
                StringTokenizer stringTokenizer=new StringTokenizer(value.toString());
                while(stringTokenizer.hasMoreTokens()){
                    word.set(stringTokenizer.nextToken());
                    context.write(word,intWritable);
                }
            }
        }
        public static class MyReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
            IntWritable result=new IntWritable();
            @Override
            protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int sum=0;
                for (IntWritable value : values) {
                    sum+=value.get();
                }
                result.set(sum);
                context.write(key,result);

            }
        }
    }


    @Test
    public void run() throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance(configuration,"WordCountApp");
        job.setJarByClass(WordCountApp.class);
        //map
        job.setMapperClass(WordCountApp.MyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //reduce
        job.setReducerClass(WordCountApp.MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
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

        // hadoop fs -cat /haha/word_count_out/part-r-00000


        FSDataInputStream fsDataInputStream=fileSystem.open(new Path(output.toString()+"/part-r-00000"));
        try{

            IOUtils.copyBytes(fsDataInputStream,System.out,4096,false);
        }catch (Exception e){

        }finally {
            IOUtils.closeStream(fsDataInputStream);
        }



    }
}
