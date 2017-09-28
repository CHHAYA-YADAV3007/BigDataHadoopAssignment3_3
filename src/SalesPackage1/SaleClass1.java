/*
 * SalesClass1.java 1.1
 * 
 * Compiled on 28th September 2017
 */
//package declaration
package SalesPackage1;
//importing the packages for MAPREDUCE Program
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.io.IOException;
import java.io.InterruptedIOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
/**
 * THIS CLASS will filter out the records which are having NA value in place of either first parameter company name
 * or second parameter product name in TV SET.
 * This Mapreduce program will calculate total nr of units sold for each company.
 * @author Chhaya yadav
 * 
 * VERSION 1.1
 * 
 * COMPILED ON 28th Sept 2017
 *
 */

//Class declaration
public class SalesClass1 {

//User Defined SalesMap inheriting the PARENT CLASS Mapper
    
    public static class SalesMap extends Mapper<LongWritable , Text , Text ,IntWritable > {
    
// Defining the abstract method map of Mapper class
        
        public void map(LongWritable key , Text value , Context context)
        throws IOException , InterruptedException
        
        {
//Conversion of each record of Text type into String                
            
                String line = value.toString();
                
//Splitting each record into word via | Delimiter                
                
                String[] word = line.split(Pattern.quote("|"));
                    
                IntWritable one = new IntWritable(1);
                
//Conversion of first word Company Name from String to Text                
                
                Text companyName =new Text(word[0]);
                
                
//Filtering out the records of Company Name and BRAND Name which have NA in its place.
//Here first record is company name and second word is brand name
                
                if (!(word[0].matches("NA") )&& (!word[1].matches("NA"))) {
                    
                    context.write(companyName, one);
                }
                        
                 
                    
                    
                }
                
                
        }
        
    public static class SalesReduce extends Reducer<Text ,IntWritable , Text  ,IntWritable > {
        
        public void reduce(Text key,Iterable<IntWritable>values,Context context)
        
        throws IOException ,InterruptedException
                {

//Grouping each record via Company Name and calculating the sum for each unit sold for each Company
            
            int sum = 0;
            
            for(IntWritable x : values){
                
                sum = sum + x.get();
            }
            context.write(key,new IntWritable(sum));
                
        
        }
    }
    
        
    public static void main(String[] args) throws Exception  {
        
        
        

        Configuration conf = new Configuration();
        
        Job job = new Job(conf,"sales");
        
        job.setJarByClass(SalesClass1.class);
        
        job.setMapperClass(SalesMap.class);
        
        job.setReducerClass(SalesReduce.class);
        
        job.setNumReduceTasks(1);
        
        job.setOutputKeyClass(Text.class);
        
        job.setOutputValueClass(IntWritable.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        
        job.setOutputFormatClass(TextOutputFormat.class);
        
        Path outputPath = new Path(args[1]);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        outputPath.getFileSystem(conf).delete(outputPath);
        
     System.exit(job.waitForCompletion(true)? 0 :1);
     
    }

}