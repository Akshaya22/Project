import java.io.*;
//import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class nine
{
	public static class employeeMapper extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
	     String val[]=value.toString().split("\t");
	     String employeer=val[2];
	     String case_status=val[1];
         context.write(new Text(employeer),new Text(case_status));	     
			
		}
	}
	
	public static class employeeReducer extends Reducer<Text,Text,Text,DoubleWritable>
	{
		public void reduce(Text employeer,Iterable<Text> case_status,Context context) throws IOException,InterruptedException
	 	{
			int sum1=0;
			int sum2=0;
			int total=0;
			float success=0;
			for(Text t:case_status)
			{			
			total++;
		    String casestatus=t.toString();
		    
		    if(casestatus.equals("CERTIFIED"))
		    { 
		    	sum1++;
		    }
		    if(casestatus.equals("CERTIFIED WITHDRAWN"))
		    {
		    	sum2++;
		    }
		    }
			success=sum1+sum2/total*100;
			if(success>70 && total>=1000)
	        context.write(employeer,new DoubleWritable(total));
	 	}
	    
	}
	    
	
	public static void main(String args[]) throws Exception
	{
	Configuration conf=new Configuration();
	Job job=Job.getInstance(conf,"JobTitle");
	
	job.setJarByClass(nine.class);
	job.setMapperClass(employeeMapper.class);
	job.setReducerClass(employeeReducer.class);
	job.setNumReduceTasks(1);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DoubleWritable.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job,new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}

