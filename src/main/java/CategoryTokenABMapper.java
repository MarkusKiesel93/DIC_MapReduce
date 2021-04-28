import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class CategoryTokenABMapper extends Mapper<Object, Text, CategoryAKey, TokenABValue> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // stream over all lines in one file
        // value.toString() -> whole file -> split by new line
        Arrays.stream(value.toString().split("\\r?\\n")).forEach(line -> {
            try {
                // extract CATEGORY, TOKEN, A, B
                String[] kv = value.toString().split("\t");  // former key CATEGORY split by tab
                String[] v = kv[1].split(":");  // former TOKEN, A, B split by ":" see TokenABValue.toString()
                Text category = new Text(kv[0]);
                Text token = new Text(v[0]);
                IntWritable A = new IntWritable(Integer.parseInt(v[1]));
                IntWritable B = new IntWritable(Integer.parseInt(v[2]));

                // create new Writeables
                CategoryAKey categoryToken = new CategoryAKey(category, A);
                TokenABValue tokenAB = new TokenABValue(token, A, B);

                // emit new key value pair
                // key -> (CATEGORY, A), value -> (TOKEN, A, B)
                // composite key is used for secondary sort
                context.write(categoryToken, tokenAB);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });


    }
}