import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TokenABValue implements Writable {

    // store TOKEN, A and B
    private Text token;
    private IntWritable A;
    private IntWritable B;

    // default constructor for serialization and deserialization
    public TokenABValue(){
        token = new Text();
        A = new IntWritable(0);
        B = new IntWritable(0);
    }

    public TokenABValue(Text token, IntWritable A, IntWritable B){
        this.token = token;
        this.A = A;
        this.B = B;
    }

    public Text getToken() {
        return token;
    }

    public IntWritable getA() {
        return A;
    }

    public IntWritable getB() {
        return B;
    }

    public double getAasDouble() {
        return A.get();
    }

    public double getBasDouble() {
        return B.get();
    }

    // need to be overwritten for working Writeable
    @Override
    public void readFields(DataInput in) throws IOException {
        token.readFields(in);
        A.readFields(in);
        B.readFields(in);
    }

    // need to be overwritten for working Writeable
    @Override
    public void write(DataOutput out) throws IOException {
        token.write(out);
        A.write(out);
        B.write(out);
    }

    // define how data is written to file
    @Override
    public String toString() {
        return token.toString() + ":" + A.toString() + ":" + B.toString();
    }
}