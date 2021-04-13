import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.TreeSet;

public class OutputReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

    private final static Text MERGED = new Text("XXX");

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        if (!key.equals(MERGED)) {
            context.write(key, values.iterator().next());
        } else {
            TreeSet<String> tokens = new TreeSet<>();

            for (Text val : values) {
                tokens.add(val.toString());
            }

            StringBuilder out = new StringBuilder();
            for (String t : tokens) {
                out.append(t).append(" ");
            }
            context.write(new Text(out.toString()), new Text());
        }


    }
}

