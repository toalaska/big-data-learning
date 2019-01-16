import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

//排过序的SequenceFIle
public class MyMapFile {
    private Configuration configuration;
    private FileSystem fileSystem;
    private Path file;


    @Before
    public void setUp() throws URISyntaxException, IOException {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);

//

        file = new Path("/haha/map");

    }
    @Test
    public void write() throws IOException {

//
        MapFile.Writer writer=new MapFile.Writer(configuration,fileSystem,file.toString(),Text.class,Text.class);
        writer.append(new Text("kkkkk1111111"),new Text("vvvvv"));
        IOUtils.closeStream(writer);
    }

    @Test
    public void read() throws IOException {
        MapFile.Reader reader=new MapFile.Reader(fileSystem,file.toString(),configuration);

        Writable keyCls=(Writable) ReflectionUtils.newInstance(reader.getKeyClass(),configuration);
        Writable valCls=(Writable)ReflectionUtils.newInstance(reader.getValueClass(),configuration);
        while(reader.next((WritableComparable) keyCls,valCls)){
            System.out.println("key:"+keyCls);
            System.out.println("value:"+valCls);


        }

    }
}
