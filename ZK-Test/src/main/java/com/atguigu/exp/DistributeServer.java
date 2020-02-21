package com.atguigu.exp;

import org.apache.zookeeper.*;

import java.io.IOException;

public class DistributeServer {

    private static ZooKeeper zooKeeper = null;

    //获取ZK客户端对象
    private static void getClient() throws IOException {

        zooKeeper = new ZooKeeper("hadoop102:2181,hadoop103:2181,hadoop104:2181", 2000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });
    }

    //注册节点
    private static void init(String host) throws KeeperException, InterruptedException {

        //创建临时节点
        zooKeeper.create("/servers/" + host, host.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

    }

    //业务
    private static void business(String host) {
        System.out.println(host + "上线了！！！");
    }


    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {

        //1.获取ZK客户端对象
        getClient();

        //2.注册节点（创建临时节点）
        init("hadoop104");

        //3.业务
        business("hadoop104");

        //4.阻塞
        Thread.sleep(Long.MAX_VALUE);

    }


}
