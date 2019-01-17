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



    }


    @Test
    public void run() throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        net.toalaska.WordCount wordCount =new net.toalaska.WordCount();

        wordCount.setUp();

        wordCount.run();



    }

}

