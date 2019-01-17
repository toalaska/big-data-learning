import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

public class WordCountCombiner {

    private Configuration configuration;
    private FileSystem fileSystem;
    private Path input;
    private Path output;

    @Before
    public void setUp() throws URISyntaxException, IOException {



    }


    @Test
    public void run() throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        net.toalaska.WordCountCombiner wordCount =new net.toalaska.WordCountCombiner();

        wordCount.setUp();

        wordCount.run();



    }

}

