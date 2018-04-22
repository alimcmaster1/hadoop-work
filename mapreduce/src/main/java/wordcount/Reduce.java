package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * Created by alistair on 14/04/18.
 */
public class Reduce extends Reducer<IntWritable, Text, Text, LongWritable> {

    private HashMap<String, IntWritable> wordFrequencyMap = new HashMap<>();
    private HashMap<String, IntWritable> topTenMap = new HashMap<>();

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            if (wordFrequencyMap.containsKey(value.toString().toLowerCase())) {
                IntWritable count = wordFrequencyMap.get(value.toString().toLowerCase());
                wordFrequencyMap.put(value.toString().toLowerCase(), new IntWritable(count.get() + 1));
            } else {
                wordFrequencyMap.put(value.toString().toLowerCase(), key);
            }

        }
        List<Integer> list = new ArrayList<>();
        for (Entry<String, IntWritable> entry : wordFrequencyMap.entrySet()) {
            list.add(entry.getValue().get());
        }

        Collections.sort(list, Collections.reverseOrder());
        List<Integer> items = list.subList(0, 10);
        int cutoff = items.get(9);

        for (Entry<String, IntWritable> entry : wordFrequencyMap.entrySet()) {
            if (entry.getValue().get() >= cutoff) {
                topTenMap.put(entry.getKey(), entry.getValue());
            }
        }


        for (String value : topTenMap.keySet()) {
            IntWritable freq = topTenMap.get(value);
            System.out.println(String.format("Entry in Reduced Set: %s%s", value, freq.get()));
            context.write(new Text(value), new LongWritable(freq.get()));
        }

    }

}
