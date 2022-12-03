package MapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;


/**
 * 
 */
public class RunJob {

    static enum eInf {
        COUNTER
    }

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","atguigu");
        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://hadoop102:8020");

        try {
            FileSystem fs = FileSystem.get(conf);
            int i = 0;
            long num = 1;
            long tmp = 0;
            while (num > 0) {
                i++;
                System.out.println(i+"##");
//                conf.setInt("run.counter", i);
                conf.setInt("run.counter",i);

                Job job = Job.getInstance(conf);
                //key value 的格式   第一个item为key，后面的item为value
                job.setInputFormatClass(KeyValueTextInputFormat.class);
                job.setJarByClass(RunJob.class);
                job.setMapperClass(ShortestPathMapper.class);
                job.setReducerClass(ShortestPathReducer.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);



                if (i == 1)
                    FileInputFormat.addInputPath(job, new Path("/test/shortestpath/input/"));
                else
                    FileInputFormat.addInputPath(job, new Path("/test/shortestpath/output/sp" + (i - 1)));

                Path outPath = new Path("/test/shortestpath/output/sp" + i);
                if (fs.exists(outPath)) {
                    fs.delete(outPath, true);
                }


                FileOutputFormat.setOutputPath(job, outPath);

                boolean b = job.waitForCompletion(true);
                System.out.println(b);
                if (b) {
                    num = job.getCounters().findCounter(eInf.COUNTER).getValue();
                    System.out.println(num);

                    if (num == 0) {
                        System.out.println("执行了" + i + "次，完成最短路径的计算");
                    }
                }
            }

        } catch (Exception e) {

            e.printStackTrace();
        }

    }

    /**
     * @author lanfenghu
     *
     *         @1 A (B,10) (D,5) =>
     *            A 0 (B,10) (D,5)
     *            B 10
     *            D 5
     *         @2 B 10 (C,1) (D,2) =>
     *         B 10 (C,1) (D,2)
     *         C 11
     *         D 13
     */
    public static class ShortestPathMapper extends Mapper<Text, Text, Text, Text> {

        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int conuter = context.getConfiguration().getInt("run.counter", 1);
            System.out.println(conuter);
            System.out.println(key+"~");
            System.out.println(value+"~");

            Node node = new Node();
            String distance = null;
            String str = null;
            String key1 = key.toString().split(" ")[0];
//            int index = key.toString().indexOf(":");
            String value1;
            if (conuter==1){
                value1 = key.toString().substring(2);
            }else{
                value1 = value.toString();
            }

//            System.out.println(value1);


//            String key1 = key.toString().split(" ")[0];

            // 第一次计算，填写默认距离 A:0 其他:inf
            if (conuter == 1) {
                System.out.println(key1);
                if (key1.equals("A") || key1.equals("1")) {
                    distance = "0";
                } else {
                    distance = "inf";
                }
                str = distance + " " + value1.toString();
                System.out.println(str+"123");
            } else {
                str = value1.toString();
                System.out.println(str+"12");
            }

            context.write(new Text(key1), new Text(str));

            node.FormatNode(str);

            System.out.println(node.getDistance()+"11");
            // 没走到此节点 退出
            if (node.getDistance().equals("inf"))
                return;

            System.out.println(node.getNodeNum()+"~");
            // 重新计算源点A到各点的距离
            for (int i = 0; i < node.getNodeNum(); i++) {
                String k = node.getNodeKey(i);
                String v = new String(
                        Integer.parseInt(node.getNodeValue(i)) + Integer.parseInt(node.getDistance()) + "");
                System.out.println(k+"!");
                System.out.println(v+"!");
                context.write(new Text(k), new Text(v));
            }

        }
    }

    /**
     * @author lanfenghu
     *
     *         B 10 (C,1) (D,2)
     *         B 8              =>
     *         B 8 (C,1) (D,2)
     *
     */
    public static class ShortestPathReducer extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text arg0, Iterable<Text> arg1, Context arg2) throws IOException, InterruptedException {
            System.out.println(arg0+"@@");
//            for (Text bb :
//                    arg1) {
//                System.out.println(bb+"@@");
//            }
            String min = null;
            int i = 0;
            String dis = "inf";
            Node node = new Node();



            for (Text t : arg1) {
                i++;
//                System.out.println(t.toString());
                dis = StringUtils.split(t.toString(), ' ')[0];
                System.out.println(dis+"DIS");

                // 如果存在inf节点，表示存在没有计算距离的节点。
                if(dis.equals("inf"))
                    arg2.getCounter(eInf.COUNTER).increment(1L);

                // 判断是否存在相邻节点，如果是则需要保留信息，并找到最小距离进行更新。
                String[] strs = StringUtils.split(t.toString(), ' ');
                if (strs.length > 1) {
                    node.FormatNode(t.toString());
                }

                // 第一条数据默认是最小距离
                if (i == 1) {
                    min = dis;
                } else {
                    if (dis.equals("inf"))
                        ;
                    else if (min.equals("inf"))
                        min = dis;
                    else if (Integer.parseInt(min) > Integer.parseInt(dis)) {
                        min = dis;
                    }
                }
            }

            // 有新的最小值，说明还在进行优化计算，需要继续循环计算
            System.out.println(min+"@@@@@");
            if (!min.equals("inf")) {
                System.out.println(node.getDistance()+"@@@");
                if (node.getDistance().equals("inf"))
                    arg2.getCounter(eInf.COUNTER).increment(1L);
                else {
                    if (Integer.parseInt(node.getDistance()) > Integer.parseInt(min))
                        arg2.getCounter(eInf.COUNTER).increment(1L);
                }
            }
            long value = arg2.getCounter(eInf.COUNTER).getValue();

            node.setDistance(min);

            System.out.println(node.toString()+"@@");
            arg2.write(arg0, new Text(node.toString()));

        }
    }
}
