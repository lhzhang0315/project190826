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

public class MyUDTF2 extends GenericUDTF {

    private List<String> list = new ArrayList<>();

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        //输出数据的默认列名
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("word1");
        fieldNames.add("word2");

        //输出数据的类型校验
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {

        //"hello:atguigu,bigdata:0826"
        String line = args[0].toString();

        //按照","分割
        String[] splits = line.split(",");

        //遍历splits
        for (String split : splits) {

            list.clear();

            //按照":"切割split
            String[] words = split.split(":");
            list.add(words[0]);
            list.add(words[1]);

            forward(list);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
