import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class LinkPageInputFormat extends FileInputFormat<LongWritable, Text>
{

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = new ArrayList<>();


        for (FileStatus status: listStatus(job)) {
            Path path = new Path(status.getPath().toString());
            FileSystem idx_fs = path.getFileSystem(job.getConfiguration());
            long idx_file_size = idx_fs.getFileStatus(path).getLen();
            long split_size = job.getConfiguration().getLong("mapreduce.input.indexedgz.bytespermap", 220000);

            for (long total = 0; total < idx_file_size; total += split_size) {
                if (total + split_size > idx_file_size) {
                    split_size = idx_file_size - total;
                }
                splits.add(new FileSplit(path, total, split_size, null));
            }
        }

        return splits;
    }
}
