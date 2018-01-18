import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class seventh {
	public static class MapperClass extends Mapper <LongWritable, Text, Text, IntWritable>
	{

		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			
			String[] str = value.toString().split("\t");
			String year = str[7];
			int sno=Integer.parseInt(str[0]);
		    	context.write(new Text(year), new IntWritable(sno));
		    }
	}
		public static class Part extends Partitioner < Text, IntWritable>{
		      public int getPartition(Text key,IntWritable value, int numReduceTasks)
		      {
		    	  String mon = key.toString();
					 int yr=Integer.parseInt(mon);
		        
		        
		        if(yr==2011)
		        {
		        	return 0 % numReduceTasks;
		        }
		        else if(yr==2012)
		        {
		        	return 1 % numReduceTasks;
		        }
		        else if(yr==2013)
		        {
		        	return 2 % numReduceTasks;
		        }
		        else if(yr==2014)
		        {
		        	return 3 % numReduceTasks;
		        }
		        else if(yr==2015)
		        {
		        	return 4 % numReduceTasks;
		        }
		        else
		        {
		        	return 5 % numReduceTasks;
		        }
		       
		      }
		   }
		public static class Reduceclass extends Reducer<Text,IntWritable,Text,IntWritable>
		{
		public void reduce(Text year,Iterable<IntWritable> sno,Context context) throws IOException,InterruptedException
		{   int sum=0;
			for(IntWritable f:sno)
			{
				sum=sum + 1;
			}
			
			
				
			context.write(year,new IntWritable (sum));
		}
		}
		public static void main(String args[]) throws Exception
		{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"Amount Group by Month");
		
		job.setJarByClass(seventh.class);
		job.setMapperClass(MapperClass.class);
		
		job.setReducerClass(Reduceclass.class);
		
		job.setNumReduceTasks(6);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setPartitionerClass(Part.class);
		FileInputFormat.addInputPath(job,new Path (args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
		}
}
