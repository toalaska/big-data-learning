import net.toalaska.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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

/**
 * 对文件中数字进行排序
 * 输入结果中第一列为排位，第二列为原始数据
 * mapReduce key为intWritable时 按key大小排序，key为Text时按字典序排序
 */
public class SortApp {
    //mapper
    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            IntWritable keyout = new IntWritable(Integer.parseInt(String.valueOf(value)));
            //keyout 每行的数值  为什么放在key   因为 key会被排序
            //value随便写个1
            context.write(keyout,new IntWritable(1));
        }
    }
    //reduce
    public static class MyReduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
        private static IntWritable data=new IntWritable(1);
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            for (IntWritable value : values) {
                //data为输出结果的第一列  不断递增
                //value的值为mapper输出的value  所以一直是1
                //key是输入的文件中每行的数值
                context.write(data,key);
                data=new IntWritable(data.get()+1);
            }
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
        input = new Path("hdfs://localhost:9000/input/num.txt");
        output = new Path("hdfs://localhost:9000/haha/num");

        if(fileSystem.exists(output)){
            fileSystem.delete(output,true);

        }


    }
    @Test

    public void run() throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance(configuration,"SortApp");
        job.setJarByClass(SortApp.class);
        //map
        job.setMapperClass( MyMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        //reduce
        job.setReducerClass( MyReduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        //设置输入格式
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job,input);
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
