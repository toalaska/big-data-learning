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

public class TrafficeApp {
    public static class TrafficeWritable implements Writable{
        private int upPackNum;//数据包
        private int downPackNum;
        private int upPayLoad;//数据量
        private int downPayLoad;

        public TrafficeWritable(int upPackNum, int downPackNum, int upPayLoad, int downPayLoad) {
            this.upPackNum = upPackNum;
            this.downPackNum = downPackNum;
            this.upPayLoad = upPayLoad;
            this.downPayLoad = downPayLoad;
        }

        //implements

        public void write(DataOutput dataOutput) throws IOException {
           dataOutput.writeInt(upPackNum);
           dataOutput.writeInt(downPackNum);
           dataOutput.writeInt(upPayLoad);
           dataOutput.writeInt(downPayLoad);
        }

        public void readFields(DataInput dataInput) throws IOException {
            upPackNum=dataInput.readInt();
            downPackNum=dataInput.readInt();
            upPayLoad=dataInput.readInt();
            downPayLoad=dataInput.readInt();
        }

        @Override
        public String toString() {

            return upPackNum+" "+downPackNum+" "+upPackNum+" "+downPayLoad;
        }


    }
    public static class MyMapper extends Mapper<LongWritable,Text,Text,TrafficeWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.printf("map \n");
            String[] arr=value.toString().split(" ");

            Text outKey=new Text(arr[0]);
            TrafficeWritable outVal = new TrafficeWritable(Integer.valueOf(arr[1]),Integer.valueOf(arr[2]),Integer.valueOf(arr[3]),Integer.valueOf(arr[4]));
            System.out.printf("map1111 \n");

            context.write(outKey,outVal);
        }
    }
    //FIXME 不执行
    public static class MyReducer extends Reducer<Text, TrafficeWritable, Text, TrafficeWritable>{
        @Override
        protected void reduce(Text key, Iterable<TrafficeWritable> values, Context context) throws IOException, InterruptedException {
            System.out.printf("reduce \n");

            int upPackNum = 0;
            int downPackNum = 0;
            int upPayLoad = 0;
            int downPayLoad = 0;

            for (TrafficeWritable value : values) {
                upPackNum+=value.upPackNum;
                downPackNum+=value.downPackNum;
                upPayLoad+=value.upPayLoad;
                downPayLoad+=value.downPayLoad;
            }

            context.write(key,new TrafficeWritable(upPackNum,downPackNum,upPayLoad,downPayLoad));
        }
    }

    //

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
        output = new Path("hdfs://localhost:9000/haha/traffice");

        if(fileSystem.exists(output)){
            fileSystem.delete(output,true);
        }


    }
    @Test
    public void run() throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance(configuration,"TrafficeApp");
        job.setJarByClass(TrafficeApp.class);
        // map
        job.setMapperClass( MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TrafficeWritable.class);
        // reduce
        job.setReducerClass( MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TrafficeWritable.class);
        //设置输入格式
//        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job,input);
        //设置输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,output);
        //
        boolean isok=job.waitForCompletion(true);
        System.out.println(isok);

        // hadoop fs -cat /haha/word_count_out/part-r-00000

        FSDataInputStream fsDataInputStream=fileSystem.open(new Path(output.toString()+"/part-r-00000"));

        IOUtils.copyBytes(fsDataInputStream,System.out,4096,false);

        IOUtils.closeStream(fsDataInputStream);




    }


}
