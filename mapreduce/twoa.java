import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class twoa {
	public static class MapperClass extends Mapper <LongWritable, Text, Text, LongWritable>
	{

		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			
			String[] str = value.toString().split("\t");
			String worksite = str[8];
			String job_title = str[4];
			String year= str[7];
			String case_status= str[1];
			if(job_title.contains("DATA ENGINEER") && case_status.equals("CERTIFIED"))
		    {
				String Key = worksite + "\t" + year;
				context.write(new Text(Key),new LongWritable(1));
		    }
		    }
	}
		public static class part extends Partitioner <Text, LongWritable>{
		      public int getPartition(Text key,LongWritable value, int numReduceTasks)
		      {
					 String Key[] = key.toString().split("\t");
						String yr = Key[1];
		        		        
		        if(yr.equals("2011"))
		        {
		        	return 0;
		        }
		        else if(yr.equals("2012"))
		        {
		        	return 1;
		        }
		        else if(yr.equals("2013"))
		        {
		        	return 2;
		        }
		        else if(yr.equals("2014"))
		        {
		        	return 3;
		        }
		        else if(yr.equals("2015"))
		        {
		        	return 4;
		        }
		        else
		        {
		        	return 5;
		        }
		       
		      }
		}
		      public static class ReduceClass extends Reducer<Text,LongWritable,Text,LongWritable>
				{   long max=0;
				    Text maxWord = new Text() ;
				public void reduce(Text key,Iterable<LongWritable> value,Context context) throws IOException,InterruptedException
				{   
				    long sum=0;
					for(LongWritable f:value)
					{
						sum+=f.get();
						if(sum>max)
						{
							max=sum;
							maxWord.set(key);
						}
						
					}
					
				}
				public void cleanup(Context context) throws IOException,InterruptedException
				{
						context.write(maxWord, new LongWritable(max));
				}
				}
				public static void main(String args[]) throws Exception
				{
				Configuration conf=new Configuration();
				Job job=Job.getInstance(conf,"group by worksite");
				job.setJarByClass(twoa.class);
				job.setMapperClass(MapperClass.class);
				job.setReducerClass(ReduceClass.class);
				job.setNumReduceTasks(6);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(LongWritable.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(LongWritable.class);
				job.setPartitionerClass(part.class);
				FileInputFormat.addInputPath(job,new Path(args[0]));
				FileOutputFormat.setOutputPath(job,new Path(args[1]));
				
				System.exit(job.waitForCompletion(true)? 0 : 1);
				}
		}