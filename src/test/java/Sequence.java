import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
//将小文件合并

public class Sequence {

    private Configuration configuration;
    private FileSystem fileSystem;
    private Path file;

    @Before
    public void setUp() throws URISyntaxException, IOException {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);

//        file = new Path("/haha/a_seq");
        file = new Path("/haha/a_seq_zip");

    }
    @Test
    public void testWrite() throws URISyntaxException, IOException {

        String[] arr = {
                "abcde", "fghijk"
        };
        IntWritable key = new IntWritable();
        //不压缩
//        SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, configuration, file, IntWritable.class, Text.class);
        //压缩
        SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, configuration, file, IntWritable.class, Text.class,SequenceFile.CompressionType.RECORD,new BZip2Codec());
        Text value = new Text();

        int i = 0;
        for (String s : arr) {
            key.set(i);
            value.set(s);
            writer.append(key, value);
            i++;
        }


        IOUtils.closeStream(writer);
    }

    @Test
    public void testRead() throws URISyntaxException, IOException {
        SequenceFile.Reader reader=new SequenceFile.Reader(fileSystem,file,configuration);
        Writable keyCls=(Writable)ReflectionUtils.newInstance(reader.getKeyClass(),configuration);
        Writable valCls=(Writable)ReflectionUtils.newInstance(reader.getValueClass(),configuration);
        while(reader.next(keyCls,valCls)){
            System.out.println("key:"+keyCls);
            System.out.println("value:"+valCls);
            System.out.println("pos:"+reader.getPosition());

        }
        /**
         * key:0
         * value:abcde
         * pos:154
         * key:1
         * value:fghijk
         * pos:181
         *
         */
        IOUtils.closeStream(reader);
    }
}
