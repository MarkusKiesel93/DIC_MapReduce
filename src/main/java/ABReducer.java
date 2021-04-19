import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class ABReducer extends Reducer<Text, CategoryCountValue, Text, TokenABValue> {

    public void reduce(Text key, Iterable<CategoryCountValue> values, Context context) throws IOException, InterruptedException {

        HashMap<String, Integer> categoryMap = new HashMap<>();
        int tokenCount = 0;

        // by category sum up the number of times the token appears for each category same as combiner
        // keep track of total count the token appears
        for (CategoryCountValue val : values) {
            categoryMap.merge(val.getCategory(), val.getCount(), Integer::sum);
            tokenCount += val.getCount();
        }

        // calculate the values A, B for every combination of category with the token
        // emit new key value pair
        // key -> category, value -> (token, A, B)
        for (String category : categoryMap.keySet()) {
            // A -> number of times category and token appear together (simply the count)
            IntWritable a = new IntWritable(categoryMap.get(category));
            // B -> number of times the token appears in another category (total count of token - A)
            IntWritable b = new IntWritable(tokenCount - a.get());
            TokenABValue TokenAB = new TokenABValue(key, a, b);
            context.write(new Text(category), TokenAB);
        }
    }
}