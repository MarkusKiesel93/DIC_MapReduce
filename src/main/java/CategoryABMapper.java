import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class CategoryABMapper extends Mapper<Object, Text, CategoryAKey, TokenABValue> {

    // todo: find better way instead of splitting on input use automatic input from some SplitFlie

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Arrays.stream(value.toString().split("\\r?\\n")).forEach(line -> {
            try {
                String[] kv = value.toString().split("\t");
                String[] v = kv[1].split(":");
                Text category = new Text(kv[0]);
                Text token = new Text(v[0]);
                IntWritable A = new IntWritable(Integer.parseInt(v[1]));
                IntWritable B = new IntWritable(Integer.parseInt(v[2]));

                CategoryAKey categoryToken = new CategoryAKey(category, A);
                TokenABValue tokenAB = new TokenABValue(token, A, B);

                context.write(categoryToken, tokenAB);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });


    }
}