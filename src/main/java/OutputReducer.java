import org.apache.hadoop.io.Text;

import java.io.IOException;

public class OutputReducer extends org.apache.hadoop.mapreduce.Reducer<OutputKey, Text, Text, Text> {

    private final static Text CATEGORY_TOKEN = new Text("T");

    public void reduce(OutputKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int i = 0;
        StringBuilder out = new StringBuilder();

        // for special category CATEGORY_TOKEN (merged list of tokens in the end of the file)
        if (key.getCategory().equals(CATEGORY_TOKEN)) {
            // remove duplicates
            String lastToken = "";
            for (Text token : values) {
                // make sure again only 150 values emitted
                if (i < 150 && !token.toString().equals(lastToken)) {
                    out.append(token.toString()).append(" ");
                    lastToken = token.toString();
                    i++;
                }
            }
            // emit list of all tokens separated by " "
            // key -> DISTINCT TOP 150 TOKENS OVER ALL CATEGORIES, value -> ""
            context.write(new Text(out.toString()), new Text(""));
        } else {
            for (Text tokenValue : values) {
                // make sure again only 150 values emitted
                if (i < 150) {
                    out.append(tokenValue).append(" ");
                    i++;
                }
            }
            // for each CATEGORY emit the list of all TOKEN:CHI-SQUARED values
            // key -> CATEGORY, value -> (TOKEN:CHI-SQUARED TOKEN:CHI-SQUARED ...)
            context.write(key.getCategory(), new Text(out.toString()));
        }
    }
}

