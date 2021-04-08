import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

public class MyReducer1 extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        HashMap<String, Integer> categoryMap = new HashMap<>();
        int tokenCount = 0;
        for (Text val : values) {
            categoryMap.merge(val.toString(), 1, Integer::sum);
            tokenCount++;
        }

        for (String category : categoryMap.keySet()) {
            int a = categoryMap.get(category);
            int b = tokenCount - a;
            String out = key.toString() + ":" + a + ":" + b;
            context.write(new Text(category), new Text(out));
        }
    }
}