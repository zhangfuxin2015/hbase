package zfx.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.ConcatenatedLists;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 创建一个表
 */
public class HbaseDemo {
    public static void main(String[] args) throws IOException {
        //通过通用的配置对象，构造方法创建
        //会自动加载classpath下的hadoop的配置文件以及hbase-site.xml
        Configuration configuration=HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hostname:2181");
        // DDL  操作
        HBaseAdmin hBaseAdmin=new HBaseAdmin(configuration);
        //键表
        TableName javaCart = TableName.valueOf("javaCart");
        //表的描述器
        HTableDescriptor hTableDescriptor=new HTableDescriptor(javaCart);
        //创建列族
        HColumnDescriptor hColumnDescriptor= new HColumnDescriptor("product");
        //保存版本数
        hColumnDescriptor.setMaxVersions(3);
        //在构造一个列族
        HColumnDescriptor recommand= new HColumnDescriptor("recommand");
        //创建表描述器
        hTableDescriptor.addFamily(hColumnDescriptor);
        hTableDescriptor.addFamily(recommand);
        //创建表
        hBaseAdmin.createTable(hTableDescriptor);
        //关闭客户端连接
        hBaseAdmin.close();
    }
    //插入一条数据
    @Test
    public  void  put() throws Exception {
        //通过通用的配置对象，构造方法创建
        //会自动加载classpath下的hadoop的配置文件以及hbase-site.xml
        Configuration configuration=HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hostname:2181");
        //先要拿到一个表的客户端连接对象 DML
        HTable hTable=new HTable(configuration,"javaCart");
        // 先将插入的数据 ，封装成put对象
        Put put=new Put("user_01".getBytes());
             //  表名、列的键，列的值
        put.addColumn("product".getBytes(),"product_id".getBytes(),"001".getBytes());
        put.addColumn("product".getBytes(),"product_num".getBytes(),"5".getBytes());
        put.addColumn("recommand".getBytes(),"product_id".getBytes(),"008".getBytes());
        hTable.put(put);
        hTable.close();
    }
    /**
     * 一次插入多行
     */
    public  void putList() throws Exception {
        //通过通用的配置对象，构造方法创建
        //会自动加载classpath下的hadoop的配置文件以及hbase-site.xml
        Configuration configuration=HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hostname:2181");
        //先要拿到一个表的客户端连接对象 DML
        HTable hTable=new HTable(configuration,"javaCart");
        // 先将插入的数据 ，封装成put对象
        List<Put> list=new ArrayList<Put>();
        //放入行健
        Put put=new Put("user_01".getBytes());
        //  表名、列的键，列的值
        put.addColumn("product".getBytes(),"product_id".getBytes(),"002".getBytes());
        put.addColumn("product".getBytes(),"product_num".getBytes(),"4".getBytes());
        put.addColumn("recommand".getBytes(),"product_id".getBytes(),"005".getBytes());
        //放入行健
        Put put1=new Put("user_02".getBytes());
        //  表名、列的键，列的值
        put1.addColumn("product".getBytes(),"product_id".getBytes(),"003".getBytes());
        put1.addColumn("product".getBytes(),"product_num".getBytes(),"3".getBytes());
        put1.addColumn("recommand".getBytes(),"product_id".getBytes(),"004".getBytes());
        list.add(put);
        hTable.put(list);
        hTable.close();
    }

    /**
     * 删除数据 一行数据
     */
    public  void delete() throws Exception{
        //通过通用的配置对象，构造方法创建
        //会自动加载classpath下的hadoop的配置文件以及hbase-site.xml
        Configuration configuration=HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hostname:2181");
        //先要拿到一个表的客户端连接对象 DML
        HTable hTable=new HTable(configuration,"javaCart");
        Delete delete=new Delete("user_02".getBytes());
        //列族的名称 ， 列的键名
        delete.addColumn("product".getBytes(),"product_num".getBytes());
        //如果删除多行的列， 那么就要构造多个delete， 放入到list中。
        hTable.delete(delete);
        hTable.close();
    }
    /**
     * 修改数据 ,就是put数据
     */
    public  void testUpdate() throws  Exception{
        //通过通用的配置对象，构造方法创建
        //会自动加载classpath下的hadoop的配置文件以及hbase-site.xml
        Configuration configuration=HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hostname:2181");
        //先要拿到一个表的客户端连接对象 DML
        HTable hTable=new HTable(configuration,"javaCart");
        // 先将插入的数据 ，封装成put对象
        Put put=new Put("user_01".getBytes());
        //  表名、列的键，列的值
        put.addColumn("product".getBytes(),"product_id".getBytes(),"001".getBytes());
        put.addColumn("product".getBytes(),"product_num".getBytes(),"888".getBytes());
        put.addColumn("recommand".getBytes(),"product_id".getBytes(),"0028".getBytes());
        hTable.put(put);
        hTable.close();
    }
    /**
     * 查询数据
     */
    public  void Get() throws  Exception{
        Configuration configuration=HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hostname:2181");
        //先要拿到一个表的客户端连接对象 DML
        HTable hTable=new HTable(configuration,"javaCart");
        Get get=new Get("user_01".getBytes());
        //指定要查询的列
        get.addColumn("product".getBytes(),"product_id".getBytes());
        get.addColumn("product".getBytes(),"product_num".getBytes());
        Result result=hTable.get(get);
        byte[] p1byte=result.getValue("product".getBytes(),"product_id".getBytes());
        byte[] n1byte=result.getValue("product".getBytes(),"product_num".getBytes());
        System.out.println(p1byte.toString());
        System.out.println(n1byte.toString());
        hTable.close();
    }
    /**
     * scan   整个查询
     */
    public  void scan() throws  Exception{
        Configuration configuration=HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hostname:2181");
        //先要拿到一个表的客户端连接对象 DML
        HTable hTable=new HTable(configuration,"javaCart");
        //封装scan查询的参数
        // scan 查询，含头不含尾
        Scan scan=new Scan("user_01".getBytes(),"user_02".getBytes());
        ResultScanner resultScanner=hTable.getScanner(scan);
        Iterator<Result> result=resultScanner.iterator();
        while (result.hasNext()){
            Result re= result.next();
            byte[] p1=re.getValue("product".getBytes(),"product_id".getBytes());
            byte[] n1=re.getValue("product".getBytes(),"product_num".getBytes());
            System.out.println(p1.toString());
            System.out.println(n1.toString());
        }
        hTable.close();
    }

    /**
     * 高级查询方法
     * 查询设置更复杂的条件
     */
    public  void testPreFilter() throws  Exception{
        Configuration configuration=HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hostname:2181");
        //先要拿到一个表的客户端连接对象 DML
        HTable hTable=new HTable(configuration,"javaCart");

        Scan scan=new Scan("user_01".getBytes(),"user_02".getBytes());
        //往scan 中添加过滤器
        // 针对行健进行过滤，
        Filter filter=new PrefixFilter("user".getBytes());
        scan.setFilter(filter);
        ResultScanner scann=hTable.getScanner(scan);
        Iterator<Result> iterator = scann.iterator();
        while (iterator.hasNext()){
            System.out.println(iterator.next());
        }

    }

    /**
     * 行的过滤器
     * @throws Exception
     */
    public  void testRowFilter() throws  Exception{
        Configuration configuration=HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hostname:2181");
        //先要拿到一个表的客户端连接对象 DML
        HTable hTable=new HTable(configuration,"javaCart");

        Scan scan=new Scan("user_01".getBytes(),"user_02".getBytes());
        // 这种 做等值比较。
        ByteArrayComparable bac=new BinaryComparator("user_01".getBytes());
        Filter filter=new RowFilter(CompareFilter.CompareOp.EQUAL,bac);
        scan.setFilter(filter);
        // 返回行健等于user_01的数据
        ResultScanner scann=hTable.getScanner(scan);
        Iterator<Result> iterator = scann.iterator();
        while (iterator.hasNext()){
            System.out.println(iterator.next());
        }
    }
    /**
     * 列的过滤器
     */
    public void testColumFileter() throws  Exception{
        Configuration configuration=HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hostname:2181");
        //先要拿到一个表的客户端连接对象 DML
        HTable hTable=new HTable(configuration,"javaCart");
        Scan scan=new Scan("user_01".getBytes(),"user_02".getBytes());
        scan.addFamily("product".getBytes());
        //like 查询
        SubstringComparator bac=new SubstringComparator("8");
        // 过滤器 如果碰到某一行没有包含这个字段，那么 也会返回
        SingleColumnValueFilter filter=new SingleColumnValueFilter("product".getBytes(),"product_id".getBytes(), CompareFilter.CompareOp.EQUAL,bac);
        // 如果字段不包含，也被过滤到。
        filter.setFilterIfMissing(true);
        scan.setFilter(filter);
        // 返回 列的值中包含8的 东西。
        ResultScanner scann=hTable.getScanner(scan);
        Iterator<Result> iterator = scann.iterator();
        while (iterator.hasNext()){
            System.out.println(iterator.next());
        }
    }

}
