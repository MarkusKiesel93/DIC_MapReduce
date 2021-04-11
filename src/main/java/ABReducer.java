import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class ABReducer extends Reducer<Text, CategoryCountValue, Text, Text> {

    public void reduce(Text key, Iterable<CategoryCountValue> values, Context context) throws IOException, InterruptedException {

        // todo: write combine with only this top part
        HashMap<String, Integer> categoryMap = new HashMap<>();
        int tokenCount = 0;
        for (CategoryCountValue val : values) {
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