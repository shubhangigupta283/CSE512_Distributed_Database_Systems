import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
//import org.mortbay.log.Log;

public class equijoin {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String val_str = value.toString();
			String[] col_data = val_str.split(",");
			String join_col_name = col_data[1];
			output.collect(new Text(join_col_name), new Text(val_str));
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Set<String> setA = new HashSet<>();
			Set<String> setB = new HashSet<>();

			String table_name="";

			boolean flag = true;
			while(values.hasNext())
			{
				String val_str1 = values.next().toString();
				String[] col_data1 = val_str1.split(",");
				String temp_tablename = col_data1[0];

				if(flag)
				{
					table_name = temp_tablename;
					//Log.info("Shub SET TABLE_NAME : "+temp_tablename);
					flag = false;
				}


				if(temp_tablename.equals(table_name))
				{
					//Log.info("Shub add setA : "+val_str1);
					setA.add(val_str1);
				}
				else
				{
					//Log.info("Shub add setB : "+val_str1);
					setB.add(val_str1);
				}
			}
			
			for(String p : setA)
			{
				for(String q : setB)
				{
					String concat_str = p+", "+q;
					//Log.info("Shub concat : "+concat_str);
					output.collect(new Text(concat_str), null);
				}
			}	
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(equijoin.class);
		conf.setJobName("equijoin");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}