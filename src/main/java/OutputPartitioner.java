import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OutputPartitioner extends Partitioner<OutputKey, IntWritable> {

    // partition by CATEGORY
    @Override
    public int getPartition(OutputKey key, IntWritable value, int numPartitions) {
        return Math.abs(key.getCategory().hashCode() % numPartitions);
    }
}