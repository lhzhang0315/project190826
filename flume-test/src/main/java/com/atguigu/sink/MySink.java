package com.atguigu.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySink extends AbstractSink implements Configurable {

    //创建Logger对象
    private Logger logger = LoggerFactory.getLogger(MySink.class);

    //前后缀声明
    private String prefix;
    private String subfix;

    @Override
    public void configure(Context context) {

        prefix = context.getString("prefix", "hello-");
        subfix = context.getString("subfix");
    }

    @Override
    public Status process() throws EventDeliveryException {

        //1.定义状态信息
        Status status;

        //2.获取Channel
        Channel channel = getChannel();

        //3.从Channel获取事务
        Transaction tx = channel.getTransaction();

        //4.开启事务
        tx.begin();

        try {
            //5.从Channel中获取数据（注意：TryCatch）
            Event event = channel.take();

            if (event != null) {
                //6.处理数据（打印到控制台）
                logger.info(prefix + new String(event.getBody()) + subfix);

            } else {
                Thread.sleep(2000);
            }

            //7.提交
            tx.commit();

            //8.更改状态信息
            status = Status.READY;
        } catch (Exception e) {

            //9.Catch回滚 更改状态信息
            tx.rollback();
            status = Status.BACKOFF;

            e.printStackTrace();

        } finally {

            //10.Finally  事务关闭
            tx.close();
        }

        return status;
    }


}
