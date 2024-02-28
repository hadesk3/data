import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ASEANCaseCount {

    public static class ASEANMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private final Text country = new Text();
        private final DoubleWritable cases = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the TSV line and extract relevant fields
            String[] fields = value.toString().split("\t");

            // Check if the country belongs to ASEAN (South-East Asia)
            if (fields.length > 1 && fields[1].equals("South-East Asia")) {
                country.set(fields[0]);
                // Remove commas and handle decimal part
                String casesString = fields[2].replaceAll(",", "");
                cases.set(Double.parseDouble(casesString));
                context.write(country, cases);
            }
        }
    }

    public static class ASEANReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private final DoubleWritable totalCases = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            totalCases.set(sum);
            context.write(key, totalCases);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ASEANCaseCount");

        job.setJarByClass(ASEANCaseCount.class);
        job.setMapperClass(ASEANMapper.class);
        job.setReducerClass(ASEANReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/phamha/lab03/input/WHO-COVID-19-20210601-213841.tsv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/phamha/lab03/output-java-new/"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

