import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CategoryAPartitioner extends Partitioner<CategoryAKey, IntWritable> {

    @Override
    public int getPartition(CategoryAKey key, IntWritable value, int numPartitions) {
        return Math.abs(key.getCategory().hashCode() % numPartitions);
    }

}