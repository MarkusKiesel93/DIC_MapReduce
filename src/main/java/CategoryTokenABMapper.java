import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class CategoryTokenABMapper extends Mapper<Object, Text, CategoryTokenKey, TokenABValue> {

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

                CategoryTokenKey categoryToken = new CategoryTokenKey(category, token);
                TokenABValue tokenAB = new TokenABValue(token, A, B);

                context.write(categoryToken, tokenAB);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });


    }
}