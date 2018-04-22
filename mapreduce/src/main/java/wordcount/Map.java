package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by alistair on 14/04/18.
 */

public class Map extends Mapper<LongWritable, Text, IntWritable, Text> {


    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] words = line.split("[^\\w']+");

        for (String word : words) {
            if (!(word.toLowerCase().equals(word)) && word.length() > 7) {
                context.write(new IntWritable(1), new Text(word));
            }
        }


    }

}
