package com.atguigu.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class MyUDTF extends GenericUDTF {

    private List<String> list = new ArrayList<>();

    //初始化方法：定义当前函数的返回值类型以及列名（可以被别名所替代）
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        //输出数据的默认列名
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("word");

        //输出数据的类型校验
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    //处理数据的方法,每一条数据调用一次
    @Override
    public void process(Object[] args) throws HiveException {

        //取出传入的参数[line,","]
        String line = args[0].toString();
        String splitKey = args[1].toString();

        //切割数据
        String[] words = line.split(splitKey);

        //遍历words写出
        for (String word : words) {

            //写出操作
            list.clear();
            list.add(word);
            forward(list);
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
