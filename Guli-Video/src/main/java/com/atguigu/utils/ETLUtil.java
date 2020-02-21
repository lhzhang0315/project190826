package com.atguigu.utils;

public class ETLUtil {

    /**
     * 处理数据的方法
     * 1.按照“\t”分割，数组长度小于9的直接过滤掉
     * 2.将类别列中的空格去掉
     * 3.将关联视频ID中的“\t”替换为“&”
     *
     * @param line 读入的数据
     */
    public static String etlString(String line) {

        StringBuffer sb = new StringBuffer();

        //1.按照“\t”分割字符串
        String[] fields = line.split("\t");

        //2.判断长度是否符合
        if (fields.length < 9) {
            return null;
        }

        //3.将类别列中的空格去掉
        fields[3] = fields[3].replaceAll(" ", "");

        //4.遍历fields重新拼接字符串
        for (int i = 0; i < fields.length; i++) {

            //a.非关联视频ID字段
            if (i < 9) {
                //最后一个字段
                if (i == fields.length - 1) {
                    sb.append(fields[i]);
                } else {
                    sb.append(fields[i]).append("\t");
                }
            } else {
                //b.关联视频ID字段
                if (i == fields.length - 1) {
                    sb.append(fields[i]);
                } else {
                    sb.append(fields[i]).append("&");
                }
            }
        }

        return sb.toString();
    }


}