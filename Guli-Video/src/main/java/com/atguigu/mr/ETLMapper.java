package com.atguigu.mr;

import com.atguigu.utils.ETLUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    //定义Value
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.获取value并转换为字符串
        String line = value.toString();

        //2.处理数据
        String etlStr = ETLUtil.etlString(line);

        //3.判断etlStr是否为Null
        if (etlStr == null) {
            return;
        }

        //4.写出
        v.set(etlStr);
        context.write(NullWritable.get(), v);

    }
}
