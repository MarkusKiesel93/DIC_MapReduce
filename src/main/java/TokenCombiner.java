import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class TokenCombiner extends Reducer<Text, CategoryCountValue, Text, CategoryCountValue> {

    public void reduce(Text key, Iterable<CategoryCountValue> values, Context context) throws IOException, InterruptedException {

        // for each category sum up the counts
        // at most 24 keys by combiner
        HashMap<String, Integer> categoryMap = new HashMap<>();
        for (CategoryCountValue val : values) {
            categoryMap.merge(val.getCategory(), val.getCount(), Integer::sum);
        }

        // emit key -> TOKEN, value -> (CATEGORY, COUNT)
        // COUNT is sum of all counts by combiner to reduce number of lines in output
        for (String category : categoryMap.keySet()) {
            Text c = new Text(category);
            IntWritable count = new IntWritable(categoryMap.get(category));
            CategoryCountValue value = new CategoryCountValue(c, count);
            context.write(key, value);
        }
    }
}
