package com.atguigu.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.ArrayList;

public class MySource extends AbstractSource implements Configurable, PollableSource {

    //定义相应的属性
    private String prefix;
    private String subfix;
    private Long delay;

    //读取配置文件
    @Override
    public void configure(Context context) {

        //获取配置信息
        prefix = context.getString("prefix", "hello-");
        delay = context.getLong("delay", 0L);
        subfix = context.getString("subfix");
    }

    //读取数据创建Event，并发送至Channel
    @Override
    public Status process() throws EventDeliveryException {

        //定义返回值状态
        Status status;

        try {
            ArrayList<Event> events = new ArrayList<>();

            //1.读取数据[atguigu0,atguigu1....atguigu4]
            for (int i = 0; i < 5; i++) {

                //封装Event
                SimpleEvent simpleEvent = new SimpleEvent();
                simpleEvent.setBody((prefix + "atguigu" + i + "-" + subfix).getBytes());

                //将事件添加至集合
                events.add(simpleEvent);
            }

            //批量提交
            getChannelProcessor().processEventBatch(events);

            Thread.sleep(delay);

            status = Status.READY;

        } catch (InterruptedException e) {
            e.printStackTrace();
            status = Status.BACKOFF;
        }

        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }


}
