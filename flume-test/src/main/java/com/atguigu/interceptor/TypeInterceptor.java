package com.atguigu.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TypeInterceptor implements Interceptor {

    private List<Event> list = new ArrayList<>();

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        //1.获取头信息&数据信息
        Map<String, String> header = event.getHeaders();
        String body = new String(event.getBody());

        //2.根据数据类型决定往头信息中添加不同的信息
        if (body.contains("info")) {
            header.put("topic", "info");
        } else {
            header.put("topic", "error");
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        //1.清空集合
        list.clear();

        //2.遍历events添加头信息
        for (Event event : events) {

            list.add(intercept(event));

        }

        return list;
    }

    @Override
    public void close() {

    }

    public static class builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

}
