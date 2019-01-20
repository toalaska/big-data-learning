package hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseTest {

    public static void main(String[] args) throws IOException {

        Configuration configuration = HBaseConfiguration.create();
//        configuration.addResource(new Path("C:\\Users\\Myra\\wsl\\hbase-1.4.9\\conf\\hbase-site.xml"));

//        configuration.set("hbase.zookeeper.quorum","127.0.0.1");  //hbase 服务地址
//        configuration.set("hbase.zookeeper.property.clientPort","2181"); //端口号

        Connection conn=ConnectionFactory.createConnection(configuration);


        Admin admin=conn.getAdmin();
        listTable(admin);
//add
        TableName tableName = add(admin);

    //查看 列
        HTableDescriptor htd=new HTableDescriptor(tableName);
        for (HColumnDescriptor family : admin.getTableDescriptor(tableName).getFamilies()) {
            System.out.println("family： "+ family.getNameAsString()+" version: "+family.getMaxVersions());

        }
        //修改列族


//        htd.addFamily(new HColumnDescriptor("name"));
        htd.addFamily(new HColumnDescriptor("age"));
//        htd.removeFamily(Bytes.toBytes("score"));
        admin.modifyTable(tableName,htd);

        //获取 数据
        Table tab=conn.getTable(TableName.valueOf("haha"));
        Put put = new Put(Bytes.toBytes("sg"));

        put.addColumn(Bytes.toBytes("age"),Bytes.toBytes(""),Bytes.toBytes("30"));
        put.addColumn(Bytes.toBytes("score"),Bytes.toBytes(""),Bytes.toBytes("100"));
        tab.put(put);

        conn.close();



    }

    private static TableName add(Admin admin) throws IOException {
        TableName tableName = TableName.valueOf("haha");
        if(admin.tableExists(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        //创建表
        HTableDescriptor htd=new HTableDescriptor(tableName);
        //至少要有一列
        htd.addFamily(new HColumnDescriptor("name"));

        htd.addFamily(new HColumnDescriptor("score"));
        admin.createTable(htd);

        //检查结果
        listTable(admin);
        return tableName;
    }

    private static void listTable(Admin admin) throws IOException {
        for (TableName tableName : admin.listTableNames()) {
            System.out.println(tableName.getNameAsString());
        }
        System.out.println("----------------------");
    }
}
