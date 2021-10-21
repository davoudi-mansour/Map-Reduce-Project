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

public class UrlIp {

    public static ArrayList<Url> url = new ArrayList<Url>();

    public static ArrayList<url_ip> urlIp= new ArrayList<url_ip>();

    public static ArrayList<Url> finalResult = new ArrayList<Url>();



    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        // private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String urlData=value.toString();
            String urlRow[]= urlData.split(",");

            context.write(new Text(urlRow[2]), one);
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

            url.add(new Url(Integer.parseInt(String.valueOf(result)),String.valueOf(key)));

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
            context.write(new Text(urlRow[0]+","+urlRow[2]), one);
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

            urlIp.add(new url_ip(Integer.parseInt(String.valueOf(result)),String.valueOf(key)));

            context.write(key, result);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job");
        job.setJarByClass(UrlIp.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if(job.waitForCompletion(true)){


            Collections.sort(url,new urlComparator());


            try{

                FileWriter fw = new FileWriter("result1.txt");

                for(int w=1;w<=url.size();w++){


                    fw.write(url.get(w).getValue()+url.get(w).getKey()+"\n");

                }

                fw.close();

            }catch (Exception e){}

            Job job1=Job.getInstance(conf,"job1");
            job1.setJarByClass(UrlIp.class);
            job1.setMapperClass(UrlMapper.class);
            job1.setCombinerClass(UrlReducer.class);
            job1.setReducerClass(UrlReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[2]));
            if(job1.waitForCompletion(true)) {



                Collections.sort(urlIp,new url_ipComparator());

                try {
                    FileWriter fw = new FileWriter("result2.txt");

                    for(Url ut : url){
                        int counter=0;

                        String s = ut.getValue();

                        for(int i=0;i<urlIp.size();i++){

                            String sp[]=urlIp.get(i).getValue().split(",");

                            if(s.equals(sp[1])){

                               counter++;

                            }
                        }

                        Url u = new Url(counter,s);
                        finalResult.add(u);


                    }

                    Collections.sort(finalResult,new urlComparator());

                    for(int c=0;c<finalResult.size();c++){
                        fw.write(finalResult.get(c).getValue()+"--->"+finalResult.get(c).getKey()+"\n");
                    }

                    for(int c=0;c<finalResult.size();c++){
                        System.out.println(finalResult.get(c).getValue()+"--->"+finalResult.get(c).getKey());
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