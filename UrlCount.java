import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UrlCount {

   public static ArrayList<Dtime> time = new ArrayList<Dtime>();

    public static ArrayList<urlTime> url_time= new ArrayList<urlTime>();

    public static ArrayList<String> visitedUrls = new ArrayList<String>();

    public static String [] finalTime;

    public static boolean visitedBefore(String url){

        boolean flag=false;
        if(visitedUrls.contains(url.toString())){
            flag=true;
        }
        return flag;

    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        // private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String urlData=value.toString();
            String urlRow[]= urlData.split(",");

            context.write(new Text(urlRow[1]), one);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {


        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);

           // time.put(Integer.parseInt(String.valueOf(result)),String.valueOf(key));
            time.add(new Dtime(Integer.parseInt(String.valueOf(result)),String.valueOf(key)));

            context.write(key, result);
        }

    }

    public static class UrlMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        // private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String urlData=value.toString();
            String urlRow[]= urlData.split(",");
            context.write(new Text(urlRow[1]+","+urlRow[2]), one);
        }
    }

    public static class UrlReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {


        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);

            url_time.add(new urlTime(Integer.parseInt(String.valueOf(result)),String.valueOf(key)));

            context.write(key, result);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job");
        job.setJarByClass(UrlCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int counter1 = Integer.parseInt(args[3]);
        int counter2=Integer.parseInt(args[4]);


        int c1=1;
        finalTime = new String[counter1];
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        if(job.waitForCompletion(true)){


           Collections.sort(time,new timeComp());

           for(int z=0;z<counter1;z++){
               finalTime[z]=time.get(z).getValue();
           }

           try{

               FileWriter fw = new FileWriter("result1.txt");

               for(int w=1;w<=counter1;w++){


                   fw.write(finalTime[w]+"\n");

               }

               fw.close();

           }catch (Exception e){}

            Job job1=Job.getInstance(conf,"job1");
            job1.setJarByClass(UrlCount.class);
            job1.setMapperClass(UrlMapper.class);
            job1.setCombinerClass(UrlReducer.class);
            job1.setReducerClass(UrlReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[2]));
            if(job1.waitForCompletion(true)) {

                Collections.sort(url_time,new time_urlComp());

                try {
                    FileWriter fw = new FileWriter("result2.txt");

                    for(urlTime ut : url_time){

                        String s[] = ut.getValue().split(",");

                        for(int i=0;i<counter1;i++){
                            if(s[0].equals(finalTime[i])){
                                if(visitedBefore(s[1])==false){
                                    fw.write(c1+"---"+s[1]+"\n");
                                    System.out.println(c1+"---"+s[1]);
                                    visitedUrls.add(s[1]);
                                    c1++;
                                    break;
                                }

                            }
                        }
                        if(counter2+1 == c1)
                            break;
                    }
                    fw.close();

                 }catch (Exception e){}

            }
            System.exit(0);
        }
        else{
            System.exit(1);
        }
    }
}