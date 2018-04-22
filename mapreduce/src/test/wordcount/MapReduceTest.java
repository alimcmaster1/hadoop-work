package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;

/**
 * Created by alistair on 15/04/18.
 */
public class MapReduceTest {

    private Configuration conf;
    private FileSystem fileSystem;

    final String inputFileDir = "tests/input/book/";
    final String outputDirectory = "tests/output";

    Path outputFilePath;

    @Before
    public void setUp() throws IOException {

        conf = new HdfsConfiguration();
        fileSystem = org.apache.hadoop.fs.FileSystem.get(conf);
        outputFilePath = new Path(outputDirectory);
        fileSystem.delete(outputFilePath, true);

    }


    @Test
    public void testMapReduce() throws IOException, ClassNotFoundException, InterruptedException {

        final String fileName = "src/test/resources/odyssey_book_one.txt";
        final ArrayList<String> bookContent = new ArrayList<String>();

        // Use Java 8 Streams
        Stream<String> stream = Files.lines(Paths.get(fileName));
        stream.forEach(line -> bookContent.add(line));

        // Now Create this File on HDFS
        Path inputFilePath = new Path(inputFileDir, "odyssey_book_one.txt");
        FSDataOutputStream outputStream = fileSystem.create(inputFilePath);
        bookContent.forEach(line -> {
            try {
                outputStream.writeBytes(line);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        outputStream.close();

        // Set up our MapReduce Job
        Job job = new Job(conf, "testOdyssey");

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        Path outputFilePath = new Path(outputDirectory);
        FileInputFormat.setInputPaths(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        job.waitForCompletion(true);

        assertTrue(job.isSuccessful());

        // Now check output is as expected
        FileStatus[] fileStatus = fileSystem.listStatus(outputFilePath);
        for (FileStatus file : fileStatus) {
            String name = file.getPath().getName();
            if (name.contains("part-r-00000")) {
                Path filePath = new Path(outputFilePath + "/" + name);
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(fileSystem.open(filePath)));

                String line;

                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }

            }
        }

    }
}
