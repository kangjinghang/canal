package com.alibaba.otter.canal.common.zookeeper.running;

import java.util.Map;

/**
 * {@linkplain ServerRunningMonitor}管理容器，使用static进行数据全局共享
 *  canal 会为每一个 destination 创建一个 CanalInstance，每个 CanalInstance 都会由一个 ServerRunningMonitor 来进行监控。而 ServerRunningMonitor 统一由 ServerRunningMonitors 进行管理。
 * @author jianghang 2012-12-3 下午09:32:06
 * @version 1.0.0
 */
public class ServerRunningMonitors {
    // 除了 CanalInstance 需要监控，CanalServer 本身也需要监控，封装了 canal server 监听的 ip 和端口等信息。
    private static ServerRunningData serverData;
    private static Map               runningMonitors; // <String,
                                                      // ServerRunningMonitor>

    public static ServerRunningData getServerData() {
        return serverData;
    }

    public static Map<String, ServerRunningMonitor> getRunningMonitors() {
        return runningMonitors;
    }

    public static ServerRunningMonitor getRunningMonitor(String destination) {
        return (ServerRunningMonitor) runningMonitors.get(destination);
    }

    public static void setServerData(ServerRunningData serverData) {
        ServerRunningMonitors.serverData = serverData;
    }

    public static void setRunningMonitors(Map runningMonitors) {
        ServerRunningMonitors.runningMonitors = runningMonitors;
    }

}
