import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.LinkedHashMap;
import java.util.TreeSet;

public class OutputReducer extends org.apache.hadoop.mapreduce.Reducer<OutputKey, Text, Text, Text> {

    private final static Text TOKEN = new Text("T");

    public void reduce(OutputKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder out = new StringBuilder();

        if (key.getCategory().equals(TOKEN)) {
            String lastToken = "";
            for (Text token : values) {
                if (!token.toString().equals(lastToken)) {
                    out.append(token.toString()).append(" ");
                    lastToken = token.toString();
                }
            }
            context.write(new Text(out.toString()), new Text(""));
        } else {
            for (Text tokenValue : values) {
                out.append(tokenValue).append(" ");
            }
            context.write(key.getCategory(), new Text(out.toString()));
        }
    }
}

