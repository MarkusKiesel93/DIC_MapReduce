import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class MyMapper2 extends Mapper<Object, Text, Text, Text> {

    // todo: find better way instead of splitting on input use automatic input from some SplitFlie

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] kv = value.toString().split("\t");
        context.write(new Text(kv[0]), new Text(kv[1]));
    }
}