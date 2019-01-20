import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

/**
 * 输入文件有两列
 * 对文件中数字进行排序，第一列相等  则按第二列进行排序
 * <p>
 * mapReduce key为intWritable时 按key大小排序，key为Text时按字典序排序
 */
public class SortTwiceApp {
    //bean
    public static class IntPair implements WritableComparable<IntPair> {
        private int first = 0;
        private int second = 0;

        public IntPair(int left, int right) {
            this.first=left;
            this.second=right;
        }

        //这里非常重要  因为对key排序  调用的就是compareTo
        public int compareTo(IntPair o) {
            if (first != o.first) {
                return first - o.first;
            } else if (second != o.second) {
                return first - o.second;
            } else {
                return 0;
            }
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(first);
            dataOutput.writeInt(second);
        }

        public void readFields(DataInput dataInput) throws IOException {
            first = dataInput.readInt();
            second = dataInput.readInt();
        }

        public int getFirst() {
            return first;
        }
    }

    //mapper
    public static class MyMapper extends Mapper<LongWritable, Text, IntPair, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer stringTokenizer=new StringTokenizer(value.toString());
            while(stringTokenizer.hasMoreTokens()){
                int left= Integer.parseInt(stringTokenizer.nextToken());
                if(stringTokenizer.hasMoreTokens()){
                    int right= Integer.parseInt(stringTokenizer.nextToken());
                     //IntPair 作为key
                    //第二列的值作为 value
                    System.out.println("left:"+left+";  right: "+right);
                    context.write(new IntPair(left,right),new IntWritable(right));
                }

            }

        }
    }

    //reduce
    //FIXME  不执行
    public static class MyReduce extends Reducer<IntPair, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("reduce: "+key.getFirst()+"");
            context.write(new Text("-----------------"),null);
            for (IntWritable value : values) {
                //第一列作为 key    第二列作为 value
                context.write(new Text(String.valueOf(key.getFirst())),value);
            }
        }
    }
    //分组函数   比较原来的key  而不是组合的key
    public static class GroupingComparator implements RawComparator<IntPair>{

        public int compare(IntPair o1, IntPair o2) {
            return o1.getFirst()-o2.getFirst();
        }

        public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
            return WritableComparator.compareBytes(bytes,i,Integer.SIZE/8,bytes1,i2,Integer.SIZE/8);
        }
    }

    //开始测试
    private Configuration configuration;
    private FileSystem fileSystem;
    private Path input;
    private Path output;

    @Before
    public void setUp() throws URISyntaxException, IOException {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);

//       这里的path要以hdfs://localhost:9000  开头
        input = new Path("hdfs://localhost:9000/input/traffice.txt");
        output = new Path("hdfs://localhost:9000/haha/traffice_out");

        if (fileSystem.exists(output)) {
            fileSystem.delete(output, true);

        }
    }

    @Test

    public void run() throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(configuration, "SortTwiceApp");
        job.setJarByClass(SortTwiceApp.class);
        //map
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        //reduce
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置 分组
        job.setSortComparatorClass(GroupingComparator.class);
//        //设置输入格式
//        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, input);
        //设置输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, output);
        //
        boolean isok = job.waitForCompletion(true);
        System.out.println(isok);

        // hadoop fs -ls /haha/nums



    }

}
