package wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by alistair on 14/04/18.
 */

public class Map extends Mapper<LongWritable, Text, IntWritable, Text> {


  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    String[] words = line.split("[^\\w']+");

    for (String word : words) {
      context.write(new IntWritable(1) , new Text(word));
    }
  }

}
