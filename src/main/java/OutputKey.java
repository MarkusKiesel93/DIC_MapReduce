import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OutputKey implements WritableComparable<OutputKey> {

    // store CATEGORY + (CHI-SQUARED OR TOKEN)
    private Text category;
    private Text valueToken;

    // default constructor for serialization and deserialization
    public OutputKey(){
        category = new Text();
        valueToken = new Text();
    }

    public OutputKey(Text category, Text valueToken) {
        this.category = category;
        this.valueToken = valueToken;
    }

    public Text getCategory() {
        return category;
    }

    public Text getValueToken() {
        return valueToken;
    }

    // need to be overwritten for working Writeable
    @Override
    public void write(DataOutput out) throws IOException {
        category.write(out);
        valueToken.write(out);
    }

    // need to be overwritten for working Writeable
    @Override
    public void readFields(DataInput in) throws IOException {
        category.readFields(in);
        valueToken.readFields(in);
    }

    // for writing output key to file
    @Override
    public String toString() {
        return category.toString() + ":" + valueToken.toString();
    }

    // needed for comparing values
    @Override
    public int compareTo(OutputKey key) {
        Text CATEGORY_TOKEN = new Text("T");
        int compareValue;

        // compare if CATEGORY is the same
        compareValue = category.compareTo(key.getCategory());

        // sort the special CATEGORY_TOKEN to last place (last line in output file)
        if (compareValue != 0 && category.equals(CATEGORY_TOKEN)) {
            compareValue = 1;
        }
        if (compareValue != 0 && key.getCategory().equals(CATEGORY_TOKEN)) {
            compareValue = -1;
        }
        // sorting by value if CATEGORY the same
        if (compareValue == 0) {
            // differentiate between special category CATEGORY_TOKEN and all others
            if (category.equals(CATEGORY_TOKEN)) {
                // sort tokens alphabetically
                compareValue = valueToken.compareTo(key.getValueToken());
            } else {
                // sort CHI-SQUARED values descending
                double value1 = Double.parseDouble(valueToken.toString());
                double value2 = Double.parseDouble(key.getValueToken().toString());
                if (value1 == value2) {
                    compareValue = 0;
                } else if (value1 > value2){
                    compareValue = -1;
                } else {
                    compareValue = 1;
                }
            }
        }
        return compareValue;
    }
}