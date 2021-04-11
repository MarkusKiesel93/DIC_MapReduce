import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class ABReducer extends Reducer<Text, CategoryCountValue, Text, TokenABValue> {

    public void reduce(Text key, Iterable<CategoryCountValue> values, Context context) throws IOException, InterruptedException {

        HashMap<String, Integer> categoryMap = new HashMap<>();
        int tokenCount = 0;
        for (CategoryCountValue val : values) {
            categoryMap.merge(val.getCategory(), val.getCount(), Integer::sum);
            tokenCount += val.getCount();
        }

        for (String category : categoryMap.keySet()) {
            IntWritable A = new IntWritable(categoryMap.get(category));
            IntWritable B = new IntWritable(tokenCount - A.get());
            TokenABValue TokenAB = new TokenABValue(key, A, B);
            context.write(new Text(category), TokenAB);
        }
    }
}