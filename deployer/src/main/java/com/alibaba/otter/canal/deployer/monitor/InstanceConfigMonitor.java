package com.alibaba.otter.canal.deployer.monitor;

import com.alibaba.otter.canal.common.CanalLifeCycle;

/**
 * 监听instance file的文件变化，触发instance start/stop等操作
 * 
 * @author jianghang 2013-2-6 下午06:19:56
 * @version 1.0.1
 */
public interface InstanceConfigMonitor extends CanalLifeCycle {
    // 当需要对一个 destination 进行监听时，调用 register 方法
    void register(String destination, InstanceAction action);
    // 当取消对一个 destination 监听时，调用 unregister 方法
    void unregister(String destination);
}
