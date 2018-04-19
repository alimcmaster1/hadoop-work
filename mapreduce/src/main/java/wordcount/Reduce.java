package wordcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by alistair on 14/04/18.
 */
public class Reduce extends Reducer<IntWritable,Text, Text, LongWritable> {

  private HashMap<String, IntWritable> wordFrequencyMap = new HashMap<String, IntWritable>();

  public void reduce(IntWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    for (Text value : values) {
      if(wordFrequencyMap.containsKey(value.toString())){
        wordFrequencyMap.put(value.toString(), new IntWritable(key.get() + 1));
      }else {
        wordFrequencyMap.put(value.toString(), key);
      }

      if(wordFrequencyMap.size() > 10 ){
       int min = wordFrequencyMap.values().stream()
            .map(e -> e.get())
            .min(Integer::compare).get();
        wordFrequencyMap.entrySet().forEach(entry -> {
          if( entry.getValue().get() == min ){
            wordFrequencyMap.remove(entry.getKey());
          }});
      }
    }

    for(String value : wordFrequencyMap.keySet()){
      IntWritable freq = wordFrequencyMap.get(value);
      System.out.println(String.format("Entry in Reduced Set: %s%s",value , freq.get()));
      context.write(new Text(value), new LongWritable(freq.get()));
    }

  }


}
