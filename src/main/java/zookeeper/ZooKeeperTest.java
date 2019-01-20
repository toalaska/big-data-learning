package zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;

public class ZooKeeperTest {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zk=new ZooKeeper("localhost:2181", 30000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("event:"+watchedEvent.toString());
            }
        });

        String znode = "/test"+System.currentTimeMillis();
        zk.create(znode,"mydata".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        String res= new String(zk.getData(znode,true,null));
        System.out.println(res);
        zk.setData(znode,"new_data".getBytes(),-1);
        String res1= new String(zk.getData(znode,true,null));
        System.out.println(res1);
        zk.delete(znode,-1);
        //zk.exists 节点不存在时返回的Null
        System.out.println(zk.exists(znode,true));
    }
}
