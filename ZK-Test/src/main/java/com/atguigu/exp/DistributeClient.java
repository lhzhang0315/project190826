package com.atguigu.exp;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

public class DistributeClient {

    private static ZooKeeper zooKeeper = null;

    //获取ZK客户端对象
    private static void getClient() throws IOException {

        zooKeeper = new ZooKeeper("hadoop102:2181,hadoop103:2181,hadoop104:2181", 2000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    getChildAndWatch();
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    //监听子节点变化
    private static void getChildAndWatch() throws KeeperException, InterruptedException {

        List<String> childrens = zooKeeper.getChildren("/servers", true);

        business(childrens);
    }

    //业务
    private static void business(List<String> list) {

        for (String s : list) {
            System.out.println(s);
        }

    }

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {

        //1.获取客户端
        getClient();

        //2.监听子节点变化
        getChildAndWatch();

        //3.阻塞
        Thread.sleep(Long.MAX_VALUE);
    }

}
