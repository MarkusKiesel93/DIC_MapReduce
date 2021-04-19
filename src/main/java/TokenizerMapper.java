import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class TokenizerMapper extends Mapper<Object, Text, Text, CategoryCountValue> {

    // my Preprocessor
    private Preprocessor pp = new Preprocessor();
    // counter for 1
    private final static IntWritable one = new IntWritable(1);
    // special TOKEN used for counting the number of times a category appears
    private final static Text N = new Text("N");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // stream over all lines in one file
        // value.toString() -> whole file -> split by regex
        Arrays.stream(value.toString().split("\\r?\\n")).forEach(line -> {
            Text category;
            String reviewText;
            try{
                // parse json and extract relevant data
                JSONObject obj = new JSONObject(line);
                category = new Text(obj.getString("category"));
                reviewText = obj.getString("reviewText");

                // emit special TOKEN "N" to count the number of times a category appears
                // value -> (CATEGORY, 1)
                context.write(N, new CategoryCountValue(category, one));

                // tokenize words
                HashSet<String> tokens = pp.tokenizePreprocess(reviewText);
                for (String token : tokens) {
                    // emit: key -> TOKEN, value -> (CATEGORY, 1)
                    context.write(new Text(token), new CategoryCountValue(category, one));
                }

            } catch(JSONException | IOException | InterruptedException e){
                e.printStackTrace();
            }
        });
    }
}