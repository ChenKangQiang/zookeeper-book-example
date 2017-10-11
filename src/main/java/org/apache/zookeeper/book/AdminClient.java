package org.apache.zookeeper.book;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Date;

/**
 * @Author chenkangqiang
 * @Data 2017/10/11
 *
 * 管理客户端，用于查看系统状态
 *
 */
public class AdminClient implements Watcher {

    private ZooKeeper zk;
    private String hostPort;

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
    }


    public AdminClient(String hostPort) {
        this.hostPort = hostPort;
    }

    /**
     * 创建会话
     * @throws Exception
     */
    public void start() throws Exception {
        zk = new ZooKeeper(hostPort, 15000, this);
    }


    public void listState() throws InterruptedException, KeeperException {
        try {
            Stat stat = new Stat();
            byte[] masterData = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            System.out.println("Master: " + new String(masterData) + " since " + startDate);
        } catch (KeeperException.NodeExistsException ex) {
            System.out.println("No Master");
        }

        System.out.println("Workers:");
        for (String w : zk.getChildren("/workers", false)) {
            byte[] data = zk.getData("/workers" + w, false, null);
            String state = new String(data);
            System.out.println("\t" + w + ": " + state);
        }

        System.out.println("Tasks:");
        for (String t : zk.getChildren("/assign", false)) {
            System.out.println("\t" + t);
        }

    }


}
