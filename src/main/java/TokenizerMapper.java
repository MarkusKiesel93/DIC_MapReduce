import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TokenizerMapper extends Mapper<Object, Text, Text, MyWritable> {

    private Preprocessor pp = new Preprocessor();
    private final static IntWritable one = new IntWritable(1);
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

                // todo: only count total number, put category n -> for this the second reducer mast sort the values
                context.getCounter("CATEGORY", category.toString()).increment(1);
                context.write(N, new MyWritable(category, one));

                // tokenize words using the given deliminators
                List<String> tokens = pp.tokenizePreprocess(reviewText);
                for (String token : tokens) {
                    context.write(new Text(token), new MyWritable(category, one));
                }

            } catch(JSONException | IOException | InterruptedException e){
                e.printStackTrace();
            }
        });
    }
}