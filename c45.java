package org.myorg;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

public class tr {
    public static String newline = System.getProperty("line.separator");
/*
The callJobs Function
return type:void
Input:Integer parameter
The job:
The job of this function is to initiate a map reduce job which
counts the number of yes and no values in the input data set.
The location of the yes and no values is in column number 4 of the
input data set(Columns are counted from 0).The output is written as
two part files in HDFS format*/

    String x[] = {"/final/input", "/final/output"};
    JobConf conf = new JobConf(tr.class);
 conf.setJobName("wordcount");
 conf.set("test","4");
 conf.setOutputKeyClass(Text.class);
 conf.setOutputValueClass(IntWritable.class);
 conf.setMapperClass(Map.class);
 conf.setCombinerClass(Reduce.class);
 conf.setReducerClass(Reduce.class);
 conf.setInputFormat(TextInputFormat.class);
 conf.setOutputFormat(TextOutputFormat.class);
    Path outputDir = new Path("/final/output");
    FileSystem fs = FileSystem.get(conf);
 fs.delete(outputDir,true);
 FileInputFormat.setInputPaths(conf,new

    Path(x[j]));
 FileOutputFormat.setOutputPath(conf,new

    Path(x[j+1]));
 JobClient.runJob(conf);
System.out.println(newline+"Iteration 1 over:"+newline);
    Path pt = new Path("/final/output/part-00000");
System.out.println(newline+"We just counted the number of yes and no values in
    column 5
    of the
    input");
            System.out.println();
    fs =FileSystem.get(new

    Configuration());
    BufferedReader br = new BufferedReader(new
            InputStreamReader(fs.open(pt)));
    String line;
    line=br.readLine();
 while(line !=null)

    {
        System.out.println(line);
        line = br.readLine();
    }br.close();
    //System.out.println("We just counted the number of yes and no values in column 5
    pf the
    input");
}

    /*The totalEntropy Function
    return type:total entropy value as double
    Input parameter:An integer
    The job:
    The totalEntropy function opens the part files written by the
    call Jobs function and gets the total yes and no values
    and calculates total entropy using the formula
    Total entopy=-(number of yes values/Total number of values*-
    log(base 2)(number of yes values/Total number of values)-
    (number of no values/Total number of values*-
    log(base 2)(number of no values/Total number of values)*/
    public static double totalEntropy(int z) throws Exception {
        if (z == 5)
            System.out.println(newline + "Calculating total Entropy" + newline);
        Path pt = new Path("/final/output/part-00000");
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String s = "";
        double pos = 0;
        double sum = 0;
        double neg = 0;
        s = br.readLine();
        StringTokenizer st = new StringTokenizer(s);
        st.nextToken();
        pos = Integer.parseInt(st.nextToken());
        s = br.readLine();
        st = new StringTokenizer(s);
        st.nextToken();
        neg = Integer.parseInt(st.nextToken());
        sum = pos + neg;
        double entropy = -((pos / sum) * (Math.log(pos / sum) / Math.log(2))) -
                ((neg / sum) * (Math.log(neg / sum) / Math.log(2)));
        if (z == 5)
            System.out.println("The Total Entropy of the given input is " + entropy + "" + newline);
        return entropy;
    }

    /*The individualEntropy() function
    return type:void
    Input parameters:none
    The job:
    The individualEntropy function creates jobs which
    counts the number of occurrence of yes and no values of each attribute in
    each column.The output directory names are appended with {a,b,c,d} depending
    on the column number namely attributes of column 0 will have a appended to
    the output directory name and attributes of column 1 will be appended by b.
    This naming convention is used for easy file manipulation and separating the
    output based on the column numbers.The map reduce jobs are actually not created
    here instead
    a call to utit method is made which does the job creation and configuration
    part.*/
    public static void individualEntropy() throws Exception {
        for (int i = 0; i < 4; i++) {
            JobConf conf = new JobConf(tr.class);
            conf.setJobName("wordcount");
            conf.set("test", i + "");
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(IntWritable.class);
            conf.setMapperClass(Map.class);
            conf.setCombinerClass(Reduce.class);
            conf.setReducerClass(Reduce.class);
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);
            Path outputDir = new Path("/final/output" + i);
            FileSystem fs = FileSystem.get(conf);
            fs.delete(outputDir, true);
            FileInputFormat.setInputPaths(conf, new Path("/final/input"));
            FileOutputFormat.setOutputPath(conf, outputDir);
            JobClient.runJob(conf);
        }
        String fn[] = {"a", "b", "c", "d"};
        for (int i = 0; i < 4; i++) {
            java.util.Map<String, Integer> temp = new HashMap<String, Integer>();
            Path pt = new Path("/final/output" + i + "/part-00000");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String s = "";
            while ((s = br.readLine()) != null) {
                StringTokenizer st = new StringTokenizer(s);
                while (st.hasMoreTokens()) {
                    temp.put(st.nextToken(), Integer.parseInt(st.nextToken()));
                }
            }
            int t = 0;
            for (java.util.Map.Entry<String, Integer> entry : temp.entrySet()) {
                String key = entry.getKey().trim();
                utit(key, "yes", fn[i] + "" + t, i);
                t++;
                utit(key, "no", fn[i] + "" + (t), i);
                t++;
            }
        }
    }

    /*
    The fileCorrector method:
    return type:void
    Input parameters:none
    The job:
    File corrector function is used to include 0 if the count of a particular
    combination is zero.Map reduce doesn't write output of those words which has a
    count of zero.So this function opens all the files written by individual entropy
    function and writes the value as 0.
    */
    public static void filecorrector() throws Exception {
        String x[] = {"a", "b", "c", "d"};
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 6; j++) {
                Path pt = new Path("/final/output" + x[i] + "" + j + "/part-00000");
                FileSystem fs = FileSystem.get(new Configuration());
                if (fs.exists(pt)) {
                    FileStatus fp = fs.getFileStatus(pt);
                    ContentSummary cSummary = fs.getContentSummary(pt);
                    long length = cSummary.getLength();
                    System.out.println(length);
                    f(length == 0)
                    {
                        int u = j - 1;
                        Path pt1 = new Path("/final/output" + x[i] + "" + u + "/part-00000");
                        FileSystem fs1 = FileSystem.get(new Configuration());
                        BufferedReader br1 = new BufferedReader(new InputStreamReader(fs1.open(pt1)));
                        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
                        StringTokenizer st1 = new StringTokenizer(br1.readLine());
                        br.write(st1.nextToken() + " " + "0");
                        br.close();
                    }
                }
            }
        }
    }

    /*
    The fileMerger method
    return type:void
    Input parameters:none
    The job:
    Hadoop writes output in individual files.This function combines the output of
    each column in HDFS format and converts it into a single file and stores it
    locally.
    Each file is the complete output of each column in the input.
    */
    public static void fileMerger() throws Exception {
        String x[] = {"a", "b", "c", "d"};
        int i = 0, j = 0;
        java.util.Map<String, ArrayList<Integer>> m1 = new
                HashMap<String, ArrayList<Integer>>();
        ArrayList<Integer> a1 = new ArrayList<Integer>();
        for (i = 0; i < 4; i++) {
            m1 = new HashMap<String, ArrayList<Integer>>();
            PrintWriter pr = new PrintWriter("/usr/local/hadoop/list" + i + ".txt");
            for (j = 0; j < 6; ) {
                Path pt = new Path("/final/output" + x[i] + "" + j + "/part-00000");
                Path pt1 = new Path("/final/output" + x[i] + "" + (j + 1) + "/part-00000");
                FileSystem fs = FileSystem.get(new Configuration());
                FileSystem fs1 = FileSystem.get(new Configuration());
                if (fs.exists(pt)) {
                    a1 = new ArrayList<Integer>();
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                    BufferedReader br1 = new BufferedReader(new InputStreamReader(fs1.open(pt1)));
                    String s = "";
                    String z1 = "";
                    String z2 = "";
                    StringTokenizer st = new StringTokenizer(br.readLine());
                    StringTokenizer st1 = new StringTokenizer(br1.readLine());
                    String xx = st.nextToken();
                    st1.nextToken();
                    a1.add(Integer.parseInt(st.nextToken()));
                    a1.add(Integer.parseInt(st1.nextToken()));
                    m1.put(xx, a1);
                }
                j = j + 2;
            }
            for (java.util.Map.Entry<String, ArrayList<Integer>> entry : m1.entrySet()) {
                ArrayList<Integer> t1 = entry.getValue();
                pr.println(entry.getKey() + " " + t1.get(0) + " " + t1.get(1));
            }
            pr.close();
        }
        System.out.println(newline + "Starting Iteration 2:" + newline);
        System.out.println(newline + "Output file contents after second Iteration
                are :"+newline);
        for (i = 0; i < 4; i++) {
            BufferedReader br = new BufferedReader(new
                    FileReader("/usr/local/hadoop/list" + i + ".txt"));
            String gg = "";
            while ((gg = br.readLine()) != null) {
                System.out.println(gg + "" + newline);
            }
            br.close();
        }
    }

    /*File merger1 method
    return type:void
    Input parameters:none
    The Job:
    It's work is same as file merger method but does it for files from iteration 2*/
    public static void fileMerger1() throws Exception {
        java.util.Map<String, ArrayList<Integer>> m1 = new
                HashMap<String, ArrayList<Integer>>();
        ArrayList<Integer> a1 = new ArrayList<Integer>();
        String x[] = {"aa", "bb"};
        for (int i = 0; i < 2; i++) {
            for (int k = 1; k < 4; k++) {
                m1 = new HashMap<String, ArrayList<Integer>>();
                PrintWriter pr = new PrintWriter("/usr/local/hadoop/list1" + i + "" + k + ".txt");
                for (int j = 0; j < 6; ) {
                    Path pt = new Path("/final/output" + x[i] + "" + k + "" + j + "/part-00000");
                    FileSystem fs = FileSystem.get(new Configuration());
                    j++;
                    Path pt1 = new Path("/final/output" + x[i] + "" + k + "" + j + "/part-00000");
                    j++;
                    FileSystem fs1 = FileSystem.get(new Configuration());
                    if (fs.exists(pt)) {
                        a1 = new ArrayList<Integer>();
                        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
                        BufferedReader br1 = new BufferedReader(new InputStreamReader(fs1.open(pt1)));
                        String s = "";
                        String z1 = "";
                        String z2 = "";
                        StringTokenizer st = new StringTokenizer(br.readLine());
                        StringTokenizer st1 = new StringTokenizer(br1.readLine());
                        String xx = st.nextToken();
                        st1.nextToken();
                        a1.add(Integer.parseInt(st.nextToken()));
                        a1.add(Integer.parseInt(st1.nextToken()));
                        m1.put(xx, a1);
                    }
                }
                for (java.util.Map.Entry<String, ArrayList<Integer>> entry : m1.entrySet()) {
                    ArrayList<Integer> t1 = entry.getValue();
                    pr.println(entry.getKey() + " " + t1.get(0) + " " + t1.get(1));
                }
                pr.close();
            }
        }
    }

    /*The selectWinner1 method
    return type:int
    Input parameter:none
    The Job:
    The selectWinner1 function calculates information gain of all the columns
    and finds the one with the highest gain and returns the column number.
    */
    public static int selectWinner1() throws Exception {
        double entropy[] = new double[4];
        double main1 = totalEntropy(0);
        String categoryNames[] = {"Outlook", "Temperature", "Humidity", "Wind", "Decision"};
        for (int i = 0; i < 4; i++) {
            BufferedReader br = new BufferedReader(new
                    FileReader("/usr/local/hadoop/list" + i + ".txt"));
            String s = "";
            while ((s = br.readLine()) != null) {
                StringTokenizer st = new StringTokenizer(s);
//System.out.println(s);
                st.nextToken();
                double x[] = new double[2];
                double c = 0.0;
                x[0] = Double.parseDouble(st.nextToken());
                x[1] = Double.parseDouble(st.nextToken());
                c += (-((x[0] / (x[0] + x[1])) * (Math.log(x[0] / (x[0] + x[1])) / Math.log(2))));
                if (x[1] != 0.0)
                    c += (-((x[1] / (x[0] + x[1])) * (Math.log(x[1] / (x[0] + x[1])) / Math.log(2))));
                entropy[i] += ((x[0] + x[1]) / 14) * c;
            }
            entropy[i] = main1 - entropy[i];
            System.out.println(newline + " " + "The Information Gain of " + categoryNames[i] + " is
                    "+entropy[i]+" "+newline);
        }
        int maxInformationgain = 0, max = (int) entropy[0];
        for (int i = 1; i < entropy.length; i++) {
            if (max < ((int) entropy[i]))
                maxInformationgain = i;
        }
        System.out.println(newline + "The winner is " + categoryNames[maxInformationgain]
                + newline + " " + newline);
        return maxInformationgain;
    }

    /*
    The winnerEntropies method
    return type:HashMap of string and double
    Input parameter:none
    The Job:
    It stores the names of the winners of iteration 1 and entropies in a1
    hashmap and returns it.*/
    public static HashMap<String, Double> winnerEntropies() throws Exception {
        ArrayList<String> x = winnerNames();
        HashMap<String, Double> m1 = new HashMap<String, Double>();
        int nodeSplit = 0;
        BufferedReader br = new BufferedReader(new
                FileReader("/usr/local/hadoop/list" + nodeSplit + ".txt"));
        String s = "";
        while ((s = br.readLine()) != null) {
            StringTokenizer st = new StringTokenizer(s);
            String z = st.nextToken();
            if (x.contains(z)) {
                double x2 = Double.parseDouble(st.nextToken());
                double y2 = Double.parseDouble(st.nextToken());
                double x3 = -((x2 / (x2 + y2)) * (Math.log(x2 / (x2 + y2)) / Math.log(2))) - ((y2 /
                        (x2 + y2)) * (Math.log(y2 / (x2 + y2)) / Math.log(2)));
                m1.put(z + " " + (x2 + y2), x3);
            }
        }
/*for(java.util.Map.Entry<String,Double> entry:m1.entrySet())
{
System.out.println(entry.getKey()+" "+entry.getValue());
}*/
        return m1;
    }

    /*The stest2() method
    return type:ArrayList of Integers
    Input Parameter:none
    The Job:
    The stest2 method finds the columns with highest information gain
    for iteration 3 and stores the column numbers of the winners in
    arraylist and returns the arraylist.*/
    public static ArrayList<Integer> stest2() throws Exception {
//double entropy[]=new double[3];
        ArrayList<Integer> ans1 = new ArrayList<Integer>();
        ArrayList<String> names = winnerNames();
        ArrayList<String> names1 = new ArrayList<String>();
        String categoryNames[] = {"Outlook", "Temperature", "Humidity", "Wind", "Decision"};
        double totalen[] = new double[2];
        nt nodeSplit = 0, p = 0;
        java.util.Map<String, Double> m1 = winnerEntropies();
        for (java.util.Map.Entry<String, Double> entry : m1.entrySet()) {
            totalen[p] = entry.getValue();
            p++;
        }
//double main1=totalEntropy();
//System.out.println(main1);
        for (int k = 0; k < 2; k++) {
            double entropy[] = new double[3];
            int t = 0;
            for (int i = 0; i < 4; i++) {
                if (i != nodeSplit) {
                    BufferedReader br = new BufferedReader(new
                            FileReader("/usr/local/hadoop/list1" + k + "" + i + ".txt"));
                    String s = "";
                    while ((s = br.readLine()) != null) {
                        StringTokenizer st = new StringTokenizer(s);
//System.out.println(s);
                        names1.add(st.nextToken());
                        double x[] = new double[2];
                        double c = 0.0;
                        x[0] = Double.parseDouble(st.nextToken());
                        x[1] = Double.parseDouble(st.nextToken());
                        if (x[0] != 0.0)
                            c += (-((x[0] / (x[0] + x[1])) * (Math.log(x[0] / (x[0] + x[1])) / Math.log(2))));
                        if (x[1] != 0.0)
                            c += (-((x[1] / (x[0] + x[1])) * (Math.log(x[1] / (x[0] + x[1])) / Math.log(2))));
                        entropy[t] += ((x[0] + x[1]) / 5) * c;
//System.out.println(x[0]+" "+x[1]);
                    }
                    entropy[t] = totalen[k] - entropy[t];
                    t++;
                }
            }
            int ll = 0;
            for (double rr : entropy) {
                System.out.println("The information gain of " + names.get(k) + "
                        "+categoryNames[ll+1]+" is"+rr+" "+newline);
                        ll++;
            }
            int listPosition = 1;
            double max = entropy[0];
            for (int y1 = 1; y1 < entropy.length; y1++) {
                if (max < entropy[y1]) {
                    max = entropy[y1];
                    listPosition = y1;
                }
            }
            ans1.add(listPosition);
            System.out.println(newline + "The winner for " + names.get(k) + " is
                    "+categoryNames[listPosition+1]+" "+newline);
                    names1 = new ArrayList<String>();
        }
        return ans1;
    }

    public static void filecorrector1() throws Exception {
        String x[] = {"aa", "bb"};
        for (int i = 0; i < 2; i++) {
            for (int k = 0; k < 4; k++) {
                for (int j = 0; j < 6; ) {
                    Path pt = new Path("/final/output" + x[i] + "" + k + "" + j + "/part-00000");
                    FileSystem fs = FileSystem.get(new Configuration());
                    j++;
                    Path pt1 = new Path("/final/output" + x[i] + "" + k + "" + j + "/part-00000");
                    j++;
                    FileSystem fs1 = FileSystem.get(new Configuration());
                    if (fs.exists(pt)) {
                        FileStatus fp = fs.getFileStatus(pt);
                        ContentSummary cSummary = fs.getContentSummary(pt);
                        long length = cSummary.getLength();
                        FileStatus fp1 = fs1.getFileStatus(pt1);
                        ContentSummary cSummary1 = fs1.getContentSummary(pt1);
                        long length1 = cSummary1.getLength();
                        if (length == 0 && length1 == 0) {
                            fs.delete(pt, true);
                            fs1.delete(pt1, true);
                        } else if (length == 0 || length1 == 0) {
                            if (length1 == 0) {
                                BufferedReader br1 = new BufferedReader(new InputStreamReader(fs.open(pt)));
                                BufferedWriter br = new BufferedWriter(new
                                        OutputStreamWriter(fs1.create(pt1, true)));
                                StringTokenizer st = new StringTokenizer(br1.readLine());
                                br.write(st.nextToken() + " " + "0");
                                br.close();
                            } else if (length == 0) {
                                BufferedReader br1 = new BufferedReader(new InputStreamReader(fs1.open(pt1)));
                                BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
                                StringTokenizer st = new StringTokenizer(br1.readLine());
                                br.write(st.nextToken() + " " + "0");
                                br.close();
                            }
                        }

                    }
                }
            }
        }
/*The winner Names method
return type:ArrayList<String>
Input parameters:none
The Job:
The winnerNames method returns the name of the winners in the first iteration*/
        public static ArrayList<String> winnerNames () throws Exception
        {
            int nodeSplit = 0;
            BufferedReader br = new BufferedReader(new
                    FileReader("/usr/local/hadoop/list" + nodeSplit + ".txt"));
            ArrayList<String> select1 = new ArrayList<String>();
            String s = "";
            while ((s = br.readLine()) != null) {
                int flag = 0;
                StringTokenizer st = new StringTokenizer(s);
                String key = st.nextToken();
                while (st.hasMoreTokens()) {
                    int x = Integer.parseInt(st.nextToken());
                    if (x == 0)
                        flag = 1;
                }
                if (flag != 1) {
                    select1.add(key);
                }
            }
            return select1;
        }
/*The selectWinner2 method
return type:void
Input parameters:none
The job:
Same as selectwinner1 but calculates the columns with highest
information gain for the second iteration.
*/

    public static void selectWinner2() throws Exception {
        int nodeSplit = 0;
        BufferedReader br = new BufferedReader(new FileReader("list" + nodeSplit + ".txt"));
        ArrayList<String> select1 = new ArrayList<String>();
        String s = "";
        while ((s = br.readLine()) != null) {
            int flag = 0;
            StringTokenizer st = new StringTokenizer(s);
            String key = st.nextToken();
            while (st.hasMoreTokens()) {
                int x = Integer.parseInt(st.nextToken());
                if (x == 0)
                    flag = 1;
            }
            if (flag != 1) {
                select1.add(key);
            }
        }
        String fn[] = {"aa", "bb"};
        int yy = 0;
        for (String r : select1) {
            for (int i = 0; i < 4; i++) {
                int t = 0;
                if (i != nodeSplit) {
                    br = new BufferedReader(new FileReader("list" + i + ".txt"));
                    String keys = "";
                    while ((keys = br.readLine()) != null) {
                        StringTokenizer st = new StringTokenizer(keys);
                        String sm = st.nextToken();
                        utit1(r, "yes", sm, fn[yy] + "" + i + "" + t, nodeSplit, i);
                        t++;
//System.out.println(yy+" "+sm);
                        utit1(r, "no", sm, fn[yy] + "" + i + "" + t, nodeSplit, i);
                        t++;
                    }
                }
            }
            yy++;
        }
    }

    /*
    The utit method
    Input parameters:
    The string x and y stores the subsets based on which the map should count.
    String y holds the ouput file name in which the output is written.int pos
    holds the position of the input values.
    For ex:The call utit("yes","o1","sunny",4) create map reduce jobs which counts
    the total number of occurrence of sunny and yes values and writes the output in
    o1/part-00000 file.Part files are automatically created by hadoop map reduce and
    we
    can only specify the directory.
    */
    public static void utit(String x, String y, String z, int pos) throws Exception {
        JobConf conf = new JobConf(tr.class);
        conf.setJobName("wordcount");
        conf.set("test", x);
        conf.set("test1", y);
        conf.set("test2", pos + "");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(myMap.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        Path outputDir = new Path("/final/output" + z);
        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputDir, true);
        FileInputFormat.setInputPaths(conf, new Path("/final/input"));
        FileOutputFormat.setOutputPath(conf, outputDir);
        JobClient.runJob(conf);
    }

    /*The utit1 method
    return type:void
    Input parameters:String x,key1 and z holds the criteria and y holds the
    file name.pos and pos 1 are positions in the input.It works same like
    utit but with one extra string to match.*/
    public static void utit1(String x, String y, String key1, String z, int pos, int pos1)
            throws Exception {
        JobConf conf = new JobConf(tr.class);
        conf.setJobName("wordcount");
        conf.set("test", x);
        conf.set("test1", y);
        conf.set("test3", key1);
        conf.set("test4", pos1 + "");
        conf.set("test2", pos + "");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(twoMap.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        Path outputDir = new Path("/final/output" + z);
        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputDir, true);
        FileInputFormat.setInputPaths(conf, new Path("/final/input"));
        FileOutputFormat.setOutputPath(conf, outputDir);
        JobClient.runJob(conf);
    }

/*
The twoMap is mapper class used by utit1 function.
*/
public static class twoMap extends MapReduceBase implements Mapper<LongWritable,
        Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private static String N;
    private static String N1;
    private static String N3;
    private static Long N2;
    private static Long N4;

    public void configure(JobConf job)

    N =job.get("test");
    N1=job.get("test1");
    N3=job.get("test3");
    N2 =Long.parseLong(job.get("test2"));
    N4=Long.parseLong(job.get("test4"));
}

    public void map(LongWritable key, Text value, OutputCollector<Text,
            IntWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String x[] = line.split(",");
        if (x[4].equals(N1) && x[Long.valueOf(N2).intValue()].equals(N) &&
                x[Long.valueOf(N4).intValue()].equals(N3)) {
            word.set(x[Long.valueOf(N4).intValue()]);
            output.collect(word, one);
        }
    }
}

/*
The myMap is a mapper class used by utit function*/
public static class myMap extends MapReduceBase implements Mapper<LongWritable,
        Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private static String N;
    private static String N1;
    private static Long N2;
    private static final Log l1 = LogFactory.getLog(Map.class);

    public void configure(JobConf job) {
        N = job.get("test");
        N1 = job.get("test1");
        N2 = Long.parseLong(job.get("test2"));
    }

    public void map(LongWritable key, Text value, OutputCollector<Text,
            IntWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String x[] = line.split(",");
        if (x[4].equals(N1) && x[Long.valueOf(N2).intValue()].equals(N)) {
            System.out.println(newline + "Map key " + key);
            word.set(x[Long.valueOf(N2).intValue()]);
            output.collect(word, one);
        }
    }
}

/*
The map is a mapper class used by call Jobs function*/
public static class Map extends MapReduceBase implements Mapper<LongWritable,
        Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private static Long N;
    private static final Log l1 = LogFactory.getLog(Map.class);

    public void configure(JobConf job) {
        N = Long.parseLong(job.get("test"));
    }

    public void map(LongWritable key, Text value, OutputCollector<Text,
            IntWritable> output, Reporter reporter) throws IOException {
        System.out.println(newline + "Map key " + key);
        String line = value.toString();
        String x[] = line.split(",");
        word.set(x[Long.valueOf(N).intValue()]);
        output.collect(word, one);
    }
}

/*The Reduce is the common reducer class used by all mapper classes*/
public static class Reduce extends MapReduceBase implements Reducer<Text,
        IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values,
                       OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        int sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
        }
        output.collect(key, new IntWritable(sum));
    }

}

    /*The generate Rules Method
    Input parameters:none
    return type:void
    The job:
    This method opens the list files created by the previous iterations and finds
    rules from those
    files and writes it in a file called rules.txt in hdfs format.
    */
    public static void generateRules() throws Exception {
        ArrayList<String> rules = new ArrayList<String>();
        String categoryNames[] = {"Outlook", "Temperature", "Humidity", "Wind", "Decision"};
        PrintWriter pr = new PrintWriter("/usr/local/hadoop/rules.txt");
        int winner1 = selectWinner1();
        ArrayList<String> winnerNames = winnerNames();
        ArrayList<Integer> winnerPos = stest2();
//System.out.println(categoryNames[winner1]+" ");
        int j = 0;
/*for(String rt:winnerNames)
{
System.out.println(rt +" "+categoryNames[winnerPos.get(j)+1]);
j++;
}*/
        BufferedReader br = new BufferedReader(new
                FileReader("/usr/local/hadoop/list" + winner1 + ".txt"));
        String z = "";
        while ((z = br.readLine()) != null) {
            StringTokenizer st = new StringTokenizer(z);
            String t1 = st.nextToken();
            if (!(winnerNames.contains(t1))) {
                int yes_value = Integer.parseInt(st.nextToken());
                int no_value = Integer.parseInt(st.nextToken());
                if (yes_value != 0 && no_value == 0)
                    rules.add(categoryNames[winner1] + " " + t1 + " " + "yes");
                else if (no_value != 0 && yes_value == 0)
                    rules.add(categoryNames[winner1] + " " + t1 + " " + "no");
            }
        }
        int l = 0;
        for (int g : winnerPos) {
            br = new BufferedReader(new FileReader("/usr/local/hadoop/list1" + l + "" +
                    (g + 1) + ".txt"));
            while ((z = br.readLine()) != null) {
                StringTokenizer st = new StringTokenizer(z);
                String t1 = st.nextToken();
                int yes_value = Integer.parseInt(st.nextToken());
                int no_value = Integer.parseInt(st.nextToken());
                if (no_value == 0)
                    rules.add(categoryNames[winner1] + " " + winnerNames.get(l) + " " + categoryNames[g + 1] + "
                            "+t1+" "+" yes");
                else if (yes_value == 0)
                    rules.add(categoryNames[winner1] + " " + winnerNames.get(l) + " " + categoryNames[g + 1] + "
                            "+t1+" "+" no");
            }
            l++;
        }
        Path pt = new Path("/final/Rules_Generated/rule.txt");
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(pt))
            fs.delete(pt, true);
        BufferedWriter br1 = new BufferedWriter(new
                OutputStreamWriter(fs.create(pt, true)));
        System.out.println("The generated rules are :" + newline);
        for (String h1 : rules) {
            System.out.println(h1 + "" + newline);
            br1.write(h1);
            br1.newLine();
        }
        br1.close();
    }

    public static void main(String[] args) throws Exception {
        Path pt = new Path("/final/input/input.txt");
        System.out.println(newline + "The sample input is :" + newline);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new
                InputStreamReader(fs.open(pt)));
        String line;
        line = br.readLine();
        while (line != null) {
            System.out.println(line + "" + newline);
            line = br.readLine();
        }
        br.close();
        System.out.println();
        callJobs(0);
        totalEntropy(5);
        individualEntropy();
        filecorrector();
        fileMerger();
        selectWinner1();
        selectWinner2();
        filecorrector1();
        fileMerger1();
        stest2();
        generateRules();
//winnerEntropies();
    }
}
