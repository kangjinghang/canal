package com.alibaba.otter.canal.store.helper;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.store.model.Event;

/**
 * 相关的操作工具
 * 
 * @author jianghang 2012-6-19 下午05:49:21
 * @version 1.0.0
 */
public class CanalEventUtils {

    /**
     * 找出一个最小的position位置，相等的情况返回position1
     */
    public static LogPosition min(LogPosition position1, LogPosition position2) {
        if (position1.getIdentity().equals(position2.getIdentity())) {
            // 首先根据文件进行比较
            if (position1.getPostion().getJournalName().compareTo(position2.getPostion().getJournalName()) > 0) {
                return position2;
            } else if (position1.getPostion().getJournalName().compareTo(position2.getPostion().getJournalName()) < 0) {
                return position1;
            } else {
                // 根据offest进行比较
                if (position1.getPostion().getPosition() > position2.getPostion().getPosition()) {
                    return position2;
                } else {
                    return position1;
                }
            }
        } else {
            // 不同的主备库，根据时间进行比较
            if (position1.getPostion().getTimestamp() > position2.getPostion().getTimestamp()) {
                return position2;
            } else {
                return position1;
            }
        }
    }

    /**
     * 根据entry创建对应的Position对象。事实上，parser模块解析后，已经将位置信息：binlog文件，position封装到了Event中，createPosition方法只是将这些信息提取出来
     */
    public static LogPosition createPosition(Event event) {
        EntryPosition position = new EntryPosition(); // 创建一个 EntryPosition 实例，提取 event 中的位置信息
        position.setJournalName(event.getJournalName()); // event 所在的 binlog 文件
        position.setPosition(event.getPosition()); // event 所在 binlog 文件中的位置
        position.setTimestamp(event.getExecuteTime()); // event 的创建时间
        // add serverId at 2016-06-28
        position.setServerId(event.getServerId()); // event 是 mysql 主从集群哪一个实例上生成的，一般都是主库，如果从库没有配置 read-only，那么 serverId 也可能是从库
        // add gtid
        position.setGtid(event.getGtid());

        LogPosition logPosition = new LogPosition(); // 将 EntryPosition 实例封装到一个 LogPosition 对象中
        logPosition.setPostion(position);
        logPosition.setIdentity(event.getLogIdentity()); // LogIdentity 中包含了这个 event 来源的 mysql 实例的 ip 地址信息
        return logPosition;
    }

    /**
     * 根据entry创建对应的Position对象
     */
    public static LogPosition createPosition(Event event, boolean included) {
        EntryPosition position = new EntryPosition();
        position.setJournalName(event.getJournalName());
        position.setPosition(event.getPosition());
        position.setTimestamp(event.getExecuteTime());
        position.setIncluded(included);
        // add serverId at 2016-06-28
        position.setServerId(event.getServerId());
        // add gtid
        position.setGtid(event.getGtid());

        LogPosition logPosition = new LogPosition();
        logPosition.setPostion(position);
        logPosition.setIdentity(event.getLogIdentity());
        return logPosition;
    }

    /**
     * 判断当前的entry和position是否相同
     */
    public static boolean checkPosition(Event event, LogPosition logPosition) {
        EntryPosition position = logPosition.getPostion();
        boolean result = position.getTimestamp().equals(event.getExecuteTime()); // 匹配时间
        // 判断是否需要根据：binlog文件+position进行比较
        boolean exactely = (StringUtils.isBlank(position.getJournalName()) && position.getPosition() == null);
        if (!exactely) {// 精确匹配
            result &= position.getPosition().equals(event.getPosition());
            if (result) {// short path
                result &= StringUtils.equals(event.getJournalName(), position.getJournalName());
            }
        }

        return result;
    }
}
