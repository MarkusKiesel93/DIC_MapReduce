import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

public class MyReducer1 extends org.apache.hadoop.mapreduce.Reducer<Text, MyWritable, Text, Text> {

    private final static Text N = new Text("N");

    public void reduce(Text key, Iterable<MyWritable> values, Context context) throws IOException, InterruptedException {

        // todo: write combine with only this top part
        HashMap<String, Integer> categoryMap = new HashMap<>();
        int tokenCount = 0;
        for (MyWritable val : values) {
            categoryMap.merge(val.getCategory(), val.getCount(), Integer::sum);
            tokenCount += val.getCount();
        }

        for (String category : categoryMap.keySet()) {
            int a = categoryMap.get(category);
            int b = tokenCount - a;
            String out = key.toString() + ":" + a + ":" + b;
            context.write(new Text(category), new Text(out));
        }
    }
}