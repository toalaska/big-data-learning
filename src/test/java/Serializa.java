import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

import java.io.*;

public class Serializa {


    class Person implements WritableComparable<Person> {
        private Text name=new Text();
        private IntWritable age=new IntWritable();


        Person(String name,int age ){
            this.name.set(name);
            this.age.set(age);

        }

        @Override
        public String toString() {
            return this.name.toString()+this.age.toString();
        }

        public Person() {

        }


        public int compareTo(Person o) {

            int res1=name.compareTo(o.name);
            if(res1!=0){
                return res1;
            }
            int res2=age.compareTo(o.age);
            if(res2!=0){
                return res2;
            }
            return 0;

        }

        public void write(DataOutput dataOutput) throws IOException {
            name.write(dataOutput);
            age.write(dataOutput);
        }

        public void readFields(DataInput dataInput) throws IOException {
            name.readFields(dataInput);
            age.readFields(dataInput);
        }
    }


    public static class HadoopSerializationUtil{
        public static byte[] serialize(Writable w) throws IOException {
            ByteArrayOutputStream byteArrayOutputStream=new ByteArrayOutputStream();
            DataOutputStream dataOutputStream=new DataOutputStream(byteArrayOutputStream);
            w.write(dataOutputStream);
            dataOutputStream.close();
            return byteArrayOutputStream.toByteArray();
        }

        public static void deserizlise( Writable w,byte[] bytes) throws IOException {
            ByteArrayInputStream in=new ByteArrayInputStream(bytes);
            DataInputStream datain=new DataInputStream(in);
            w.readFields(datain);
            datain.close();

        }
    }
    @Test
    public void test() throws IOException {
        Person p1=new Person("sg",30);
        byte[] bytes=HadoopSerializationUtil.serialize(p1);
        Person p2=new Person();
        HadoopSerializationUtil.deserizlise(p2,bytes);
        System.out.println(p2);
    }
}
