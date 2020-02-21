package com.atguigu.udf;

import org.apache.hadoop.hive.ql.exec.UDF;


//20191111 => 2019-11-11
public class MyUDF extends UDF {

    public String evaluate(String str) {

        String year = str.substring(0, 4);
        String month = str.substring(4, 6);
        String day = str.substring(6, 8);
        return year + "-" + month + "-" + day;
    }

    public String evaluate(String str1, String str2) {
        return str1 + ":" + str2;
    }

}
