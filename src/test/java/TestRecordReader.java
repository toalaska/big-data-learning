import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/*
第次读取一条记录会调用 一次
TestRecordReader 默认是LineRecordReader ,
TextInputFormat 对应 TestRecordReader
SequenceFileInputFormat对应 SequenceFileRecordReader
*
* 分别统计数据文件中奇数行和偶数行
*
 * 碰到一个坑  因为maper ruduce之类的类都是一个内部类  一定要定义成public static
 * 否则mapreduce无法实例化类
  *
  * */
public class TestRecordReader {
    //MyRecordReader

    public static class MyRecordReader extends RecordReader<LongWritable, Text> {
        private long start;
        private long end;
        private long pos;
        private FSDataInputStream fsDataInputStream = null;
        private LongWritable key = null;
        private Text value = null;
        private LineReader lineRecordReader = null;


        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) inputSplit;
            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            Configuration configure = taskAttemptContext.getConfiguration();

            Path path = fileSplit.getPath();
            FileSystem fileSystem = path.getFileSystem(configure);
            fsDataInputStream = fileSystem.open(path);
            fsDataInputStream.seek(start);
            lineRecordReader = new LineReader(fsDataInputStream);
            pos = 1;
        }

        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (key == null) {
                key = new LongWritable();
            }
            key.set(pos);
            if (value == null) {
                value = new Text();
            }
            if (lineRecordReader.readLine(value) == 0) {
                return false;
            }
            pos++;
            return true;
        }

        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        public void close() throws IOException {
            fsDataInputStream.close();
        }
    }

    //自定义InputFormat
    public static  class MyInputFormat extends FileInputFormat<LongWritable, Text> {
        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }
        @Override
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return new MyRecordReader();
        }
    }

    //自定义Partitioner
     public static  class MyPartitioner extends Partitioner<LongWritable, Text> {

        public int getPartition(LongWritable key, Text value, int i) {
            if (key.get() % 2 == 0) {
                //偶数放到分区1
                key.set(1);
                //将输入到 reduce的key设置 成1

                return 1;
            } else {
                //奇数放到分区0

                key.set(0);
                return 0;
            }
        }
    }

    //mapper
    public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.printf("map %s:%s\n", key, value);
            //直接写出
            context.write(key, value);

        }
    }

    //reduce
    public static class MyReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
        //创建写出去的key/val
        Text outKey = new Text();
        LongWritable outVal = new LongWritable();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.printf("奇数行还是偶数行，%s\n", key);
            long sum = 0;
            for (Text value : values) {
                sum += Long.parseLong(value.toString());
            }

            if (key.get() == 0) {
                outKey.set("奇数和：");
            } else {
                outKey.set("偶数和：");
            }
            outVal.set(sum);

            context.write(outKey, outVal);

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

//       这里的path要以hdfs://localhost:9000  开头
        input = new Path("hdfs://localhost:9000/input/recorder.txt");
        output = new Path("hdfs://localhost:9000/haha/recorder.out");

        if(fileSystem.exists(output)){
            fileSystem.delete(output,true);

        }


    }
    @Test
    public void run() throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance(configuration,"TestRecordReader");
        job.setJarByClass(TestRecordReader.class);

        FileInputFormat.addInputPath(job,input);
        //设置输入格式 !!!!
        job.setInputFormatClass(MyInputFormat.class);

        //map
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置 partitioner
        job.setPartitionerClass( MyPartitioner.class);
        //设置 4个分区
        job.setNumReduceTasks(2);

        //reduce
        job.setReducerClass( MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);




        //设置输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,output);
        //
        boolean isok=job.waitForCompletion(true);
        System.out.println(isok);



        /*
        hadoop fs -ls /haha/recorder.out
        hadoop fs -cat /haha/recorder.out/part-r-00000
        hadoop fs -cat /haha/recorder.out/part-r-00001
        * */
    }


}
