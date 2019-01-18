import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

/*
dept.txt emp.txt 最后一列为部门id
* join
* */
public class ReduceJoin {
    public static class EmpOrDept implements WritableComparable{
        private String empNo ="";
        private String empName ="";
        private String deptNo="";
        private String deptName="";
        private int flag=0;

        
        public static int FLAG_DEPT=1;
        public static int FLAG_EMP=0;
        //constructer


        public EmpOrDept(String empNo, String empName, String deptNo, String deptName, int flag) {
            this.empNo = empNo;
            this.empName = empName;
            this.deptNo = deptNo;
            this.deptName = deptName;
            this.flag = flag;//1部门 0员工
        }

        public EmpOrDept(EmpOrDept e) {
            this.empNo = e.empNo;
            this.empName = e.empName;
            this.deptNo = e.deptNo;
            this.deptName = e.deptName;
            this.flag = e.flag;
        }
        //get set

        public String getEmpNo() {
            return empNo;
        }

        public void setEmpNo(String empNo) {
            this.empNo = empNo;
        }

        public String getEmpName() {
            return empName;
        }

        public void setEmpName(String empName) {
            this.empName = empName;
        }

        public void setDeptNo(String deptNo) {
            this.deptNo = deptNo;
        }

        public String getDeptName() {
            return deptName;
        }

        public void setDeptName(String deptName) {
            this.deptName = deptName;
        }

        public int getFlag() {
            return flag;
        }

        public void setFlag(int flag) {
            this.flag = flag;
        }


        //tostings

        @Override
        public String toString() {
            return "EmpOrDept{" +
                    "empNo='" + empNo + '\'' +
                    ", empName='" + empName + '\'' +
                    ", deptNo='" + deptNo + '\'' +
                    ", deptName='" + deptName + '\'' +
                    ", flag=" + flag +
                    '}';
        }

        //实现
        public int compareTo(Object o) {
            //不做排序 
            return 0;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(this.empNo);
            dataOutput.writeUTF(this.empName);
            dataOutput.writeUTF(this.deptNo);
            dataOutput.writeUTF(this.deptName);
            dataOutput.writeInt(this.flag);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.empNo = dataInput.readUTF();
            this.empName =  dataInput.readUTF();
            this.deptNo =  dataInput.readUTF();
            this.deptName = dataInput.readUTF();
            this.flag =  dataInput.readInt();
        }

        public String getDeptNo() {
            return deptNo;
        }
    }
    
    //mapper  keyin,valin,keyout,valout
    public static class MyMapper extends Mapper<LongWritable, Text,LongWritable, EmpOrDept> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.printf("keyin=%s; valin=%s\n",key,value.toString());
            //value是一行记录
            String[] arr=value.toString().split(" ");

            if(arr.length<=3){
                //dept
                EmpOrDept empOrDept=new EmpOrDept("","",arr[0],arr[1],EmpOrDept.FLAG_DEPT);
                LongWritable keyout = new LongWritable(Long.valueOf(empOrDept.getDeptNo()));
                System.out.printf("keyout1 %s\n",keyout);

                context.write(keyout,empOrDept);
            }else{
                
                //emp
                EmpOrDept empOrDept=new EmpOrDept(arr[0],arr[1],arr[7],"",EmpOrDept.FLAG_EMP);
                LongWritable keyout = new LongWritable(Long.valueOf(empOrDept.getDeptNo()));
                System.out.printf("keyout2 %s\n",keyout);

                context.write(keyout,empOrDept);



            }
            // context.write(keyout,valout);//对应 LongWritable, EmpOrDept
            
        }
    }
    
    //reducer //FIXME MyReducer不执行
    public static class MyReducer extends Reducer<LongWritable,EmpOrDept, NullWritable,Text>{

        @Override
        protected void reduce(LongWritable key, Iterable<EmpOrDept> values, Context context) throws IOException, InterruptedException {
            System.out.printf("reduce\n");
            EmpOrDept dept=null;
            ArrayList<EmpOrDept> emps =new ArrayList<EmpOrDept>();
            for (EmpOrDept value : values) {
                System.out.printf("%s\n",value);
                if(value.getFlag()==EmpOrDept.FLAG_EMP){
                    EmpOrDept emp=new EmpOrDept(value);
                    emps.add(emp);
                }else{
                    dept=new EmpOrDept(value);
                }
            }
            if(dept!=null){
                for (EmpOrDept emp : emps) {
                    //为每个emp填充部门名称
                    emp.setDeptName(dept.getDeptName());
                    context.write(NullWritable.get(),new Text(emp.toString()));
                }
            }

        }
    }
    
    
    //测试开始
    private Configuration configuration;
    private FileSystem fileSystem;
    private Path input1;
    private Path input2;
    private Path output;

    @Before
    public void setUp() throws URISyntaxException, IOException {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);

//

        input1 = new Path("hdfs://localhost:9000/input/dept.txt");
        input2 = new Path("hdfs://localhost:9000/input/emp.txt");
        output = new Path("hdfs://localhost:9000/haha/join_out");



        if(fileSystem.exists(output)){
            fileSystem.delete(output,true);

        }

    }

    @Test
    public void run() throws InterruptedException, IOException, ClassNotFoundException {
        Job job=  Job.getInstance(configuration,"ReduceJoin1");

        job.setJarByClass(ReduceJoin.class);

        job.setMapperClass(ReduceJoin.MyMapper.class);
        job.setReducerClass(ReduceJoin.MyReducer.class);


        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(EmpOrDept.class);


        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(EmpOrDept.class);



        FileInputFormat.addInputPath(job,input1);

        FileInputFormat.addInputPath(job,input2);


        FileOutputFormat.setOutputPath(job,output);

         boolean   isok = job.waitForCompletion(true);


        System.out.println(isok);


        /*
        hadoop fs -ls /haha/join_out
        hadoop fs -cat /haha/mobiles_out/part-r-00000


*/


    }
}
