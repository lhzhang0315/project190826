package com.atguigu.api;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * DDL:
 * 1.创建命名空间
 * 2.判断表是否存在
 * 3.创建表
 * 4.删除表
 * <p>
 * DML:
 * 1.新增数据（修改）
 * 2.查询数据（Get）
 * 3.查询数据（Scan）
 * ---过滤器
 * 4.删除数据
 */
public class HBaseTest {

    private static Connection connection;
    private static Admin admin;

    static {
        //创建配置信息
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        try {
            //创建连接
            connection = ConnectionFactory.createConnection(configuration);

            //创建Admin对象
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //关闭连接
    private static void close() {

        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    //第一部分：DDL
    //1.创建命名空间
    private static void createNS(String ns) throws IOException {

        //创建配置信息
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        //创建客户端对象
        HBaseAdmin admin = new HBaseAdmin(configuration);

        //创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();

        //创建命名空间
        try {
            admin.createNamespace(namespaceDescriptor);                               //
        } catch (NamespaceExistException e) {
            System.out.println(ns + "命名空间已存在！！！");
        }

        //关闭连接
        admin.close();
    }

    //2.判断表是否存在
    private static boolean isTableExist(String tableName) throws IOException {

        //判断表是否存在
        return admin.tableExists(TableName.valueOf(tableName));
    }

    //3.创建表
    private static void createTable(String tableName, String... cfs) throws IOException {

        //TODO:人为创建表可能会存在语法不规范的情况（比如忘了指定列族）；
        //TODO：还有可能存在创建的表已经存在了，所以需要针对这两种情况进行判断，如 a.  b.

        //a.判断列族是否为空
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息！！！");
            return;
        }

        //b.判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println(tableName + "表已存在！！！");
            return;
        }

        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        //循环添加列族信息
        for (String cf : cfs) {

            //创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);

            //添加列族信息
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        //创建表
        admin.createTable(hTableDescriptor);
    }

    //4.删除表
    private static void dropTable(String tableName) throws IOException {

        //判断表是否存在
        if (!isTableExist(tableName)) {
            System.out.println(tableName + "表不存在！！！");
            return;
        }

        TableName table = TableName.valueOf(tableName);

        //使表下线
        admin.disableTable(table);

        //删除表
        admin.deleteTable(table);
    }

    //5.新增数据（修改）->put 'stu','1001','info:name','value'
    private static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {

        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //创建Put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //添加数据信息
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("sex"), Bytes.toBytes("female"));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("addr"), Bytes.toBytes("shanghai"));

        //插入数据
        table.put(put);

        //关闭连接
        table.close();
    }

    //6.查询数据（Get方式）
    private static void getData(String tableName, String rowKey, String cf, String cn) throws IOException {

        //获取Table对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        //指定 列族 获取信息
//        get.addFamily(Bytes.toBytes(cf));

        //指定 列族:列 获取信息
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));

        //指定获取多个版本数据
        get.setMaxVersions(2);

        //获取数据
        Result result = table.get(get);
        //解析result
        for (Cell cell : result.rawCells()) {

            System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                    ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    ",Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }

        //关闭连接
        table.close();
    }

    //7.查询数据（Scan方式）
    private static void scanData(String tableName, String start, String stop) throws IOException {

        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //创建Scan对象
        Scan scan = new Scan(Bytes.toBytes(start), Bytes.toBytes(stop));

        //扫描数据
        ResultScanner results = table.getScanner(scan);

        //解析results
        for (Result result : results) {

            for (Cell cell : result.rawCells()) {

                System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ",Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        //关闭连接
        table.close();
    }

    //8.查询数据（Scan方式带过滤器）
    private static void scanDataWithFilter(String tableName, String cf, String cn, String value) throws IOException {

        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //创建Scan对象
        Scan scan = new Scan(Bytes.toBytes("1008"), Bytes.toBytes("11"));

        //创建Filter对象
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("7"));
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes(cn), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value));

        //创建过滤器集合
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);

        filterList.addFilter(rowFilter);
        filterList.addFilter(singleColumnValueFilter);

        //添加过滤器
        scan.setFilter(filterList);

        //扫描数据
        ResultScanner results = table.getScanner(scan);

        //解析results
        for (Result result : results) {

            for (Cell cell : result.rawCells()) {

                System.out.println("RowKey:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ",Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        //关闭连接
        table.close();
    }

    //9.删除数据
    private static void deleteData(String tableName, String rowKey, String cf, String cn) throws IOException {

        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //创建Delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //指定 列族 删除
//        delete.addFamily(Bytes.toBytes(cf));

        //指定 列族:列 删除
        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn));

        //删除数据
        table.delete(delete);

        //关闭连接
        table.close();
    }

    public static void main(String[] args) throws IOException {

//        createNS("big");

//        System.out.println(isTableExist("stu4"));
//        createTable("big:stu4", "info1", "info2");
//
//        dropTable("stu4");
//
//        System.out.println(isTableExist("stu4"));

//        putData("stu3", "1010", "info", "name", "chenliu");

//        getData("student", "1002", "info2", "addr");

//        scanData("stu3", "11", "10");

//        scanDataWithFilter("stu3", "info", "name", "aa");

        deleteData("stu3", "1017", "info", "name");

        close();
    }

}
