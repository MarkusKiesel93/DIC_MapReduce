import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class TokenCombiner extends Reducer<Text, CategoryCountValue, Text, CategoryCountValue> {

    public void reduce(Text key, Iterable<CategoryCountValue> values, Context context) throws IOException, InterruptedException {

        HashMap<String, Integer> categoryMap = new HashMap<>();
        for (CategoryCountValue val : values) {
            categoryMap.merge(val.getCategory(), val.getCount(), Integer::sum);
        }

        for (String category : categoryMap.keySet()) {
            CategoryCountValue value = new CategoryCountValue(new Text(category), new IntWritable(categoryMap.get((category))));
            context.write(key, value);
        }

    }
}
