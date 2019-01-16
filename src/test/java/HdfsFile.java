import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsFile {

    private FileSystem fileSystem;
    private Configuration configuration;

    @Before
    public void setUp() throws URISyntaxException, IOException {
        configuration = new Configuration();

        fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);

    }

    @Test
    public void mkdir() throws IOException {


        fileSystem.mkdirs(new Path("/haha"));
    }

    @Test
    public void create() throws IOException {


        FSDataOutputStream out=fileSystem.create(new Path("/haha/a.txt"));
        out.write("hehhe".getBytes());
        out.flush();
        out.close();
    }
    @Test
    public void rename() throws IOException {


        fileSystem.rename(new Path("/haha/a.txt"),new Path("/haha/b.txt"));

    }
    @Test
    public void copyFromLocal() throws IOException {


        fileSystem.copyFromLocalFile(new Path("C:\\projects\\hadoop\\pom.xml"),new Path("/haha/pom.xml"));

    }

    @Test
    public void listFiles() throws IOException {
        FileStatus[] files=fileSystem.listStatus(new Path("/haha"));
        for (FileStatus file : files) {
            System.out.println(file.getPath().toString());
            System.out.println(file.getReplication());
        }
    }

}
