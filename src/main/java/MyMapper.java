import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final static Text n = new Text("N");

    private Preprocessor pp = new Preprocessor();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // stream over all lines in one file
        // value.toString() -> whole file -> split by regex
        Arrays.stream(value.toString().split("\\r?\\n")).forEach(line -> {
            String category;
            String reviewText;
            try{
                // parse json and extract relevant data
                JSONObject obj = new JSONObject(line);
                category = obj.getString("category");
                reviewText = obj.getString("reviewText");

                context.write(n, one);
                context.write(new Text("c:" + category), one);

                // tokenize words using the given deliminators
                List<String> tokens = pp.tokenizePreprocess(reviewText);

                for (int i=0; i<tokens.size(); i++) {
                    context.write(new Text("t:" + tokens.get(i)), one);
                }
            } catch(JSONException | IOException | InterruptedException e){
                e.printStackTrace();
            }
        });
    }
}