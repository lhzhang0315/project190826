package com.atguigu.test;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestZK {

    private ZooKeeper zooKeeper = null;

    //获取ZK客户端对象
    @Before
    public void getClient() throws IOException {

        zooKeeper = new ZooKeeper("hadoop102:2181,hadoop103:2181,hadoop104:2181", 2000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
//                try {
//                    List<String> childrens = zooKeeper.getChildren("/", true);
//                    for (String children : childrens) {
//                        System.out.println(children);
//                    }
//                } catch (KeeperException | InterruptedException e) {
//                    e.printStackTrace();
//                }
//
//                //打印变化事件的类型
//                System.out.println(event.getType());
            }
        });

        System.out.println(zooKeeper);
    }

    //创建子节点
    @Test
    public void createNode() throws KeeperException, InterruptedException {

        zooKeeper.create("/bigdata1", "bigdata".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

//        Thread.sleep(Long.MAX_VALUE);
    }

    //查询子节点
    @Test
    public void getChild() throws KeeperException, InterruptedException {

        //查询数据
        List<String> childrens = zooKeeper.getChildren("/", true);

        //遍历打印
        for (String children : childrens) {

            //打印每一个节点
            System.out.println(children);
        }

        System.out.println("***************************");

        Thread.sleep(Long.MAX_VALUE);
    }


    //判断节点是否存在
    @Test
    public void existsNode() throws KeeperException, InterruptedException {

        //判断节点是否存在
        Stat stat = zooKeeper.exists("/bigdata", false);

        //打印
        System.out.println(stat == null ? "not exist" : "exist");
    }

}
