package com.alibaba.otter.canal.deployer;

import java.util.Map;
import java.util.Properties;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningData;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningListener;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitor;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitors;
import com.alibaba.otter.canal.deployer.InstanceConfig.InstanceMode;
import com.alibaba.otter.canal.deployer.monitor.InstanceAction;
import com.alibaba.otter.canal.deployer.monitor.InstanceConfigMonitor;
import com.alibaba.otter.canal.deployer.monitor.ManagerInstanceConfigMonitor;
import com.alibaba.otter.canal.deployer.monitor.SpringInstanceConfigMonitor;
import com.alibaba.otter.canal.instance.core.CanalInstanceGenerator;
import com.alibaba.otter.canal.instance.manager.PlainCanalInstanceGenerator;
import com.alibaba.otter.canal.instance.manager.plain.PlainCanalConfigClient;
import com.alibaba.otter.canal.instance.spring.SpringCanalInstanceGenerator;
import com.alibaba.otter.canal.server.CanalMQStarter;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.server.exception.CanalServerException;
import com.alibaba.otter.canal.server.netty.CanalServerWithNetty;
import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import com.google.common.collect.MigrateMap;

/**
 * canal调度控制器
 *
 * @author jianghang 2012-11-8 下午12:03:11
 * @version 1.0.0
 */
public class CanalController {

    private static final Logger                      logger   = LoggerFactory.getLogger(CanalController.class);
    private String                                   ip;
    private String                                   registerIp;
    private int                                      port;
    private int                                      adminPort;
    // 默认使用spring的方式载入，每个 destination 对应的 InstanceConfig 配置就存放于 instanceConfigs 字段中
    private Map<String, InstanceConfig>              instanceConfigs;
    private InstanceConfig                           globalInstanceConfig; // 表示canal instance 的全局配置
    private Map<String, PlainCanalConfigClient>      managerClients;
    // 监听instance config的变化
    private boolean                                  autoScan = true;
    private InstanceAction                           defaultAction; // 作用是如果配置发生了变更，默认应该采取什么样的操作
    private Map<InstanceMode, InstanceConfigMonitor> instanceConfigMonitors; // defaultAction 字段只是定义了配置发生变化默认应该采取的操作，那么总该有一个类来监听配置是否发生了变化，这就是 InstanceConfigMonitor 的作用。key 为 InstanceMode，就是为了对这两种方式的配置加载方式都进行监听。
    private CanalServerWithEmbedded                  embededCanalServer;
    private CanalServerWithNetty                     canalServer;

    private CanalInstanceGenerator                   instanceGenerator;
    private ZkClientx                                zkclientx;

    private CanalMQStarter                           canalMQStarter;
    private String                                   adminUser;
    private String                                   adminPasswd;

    public CanalController(){
        this(System.getProperties());
    }
    // 对配置文件内容解析，初始化相关成员变量，做好 canal server 的启动前的准备工作
    public CanalController(final Properties properties){
        managerClients = MigrateMap.makeComputingMap(this::getManagerClient);

        // 初始化全局参数设置 1.配置解析
        globalInstanceConfig = initGlobalConfig(properties);
        instanceConfigs = new MapMaker().makeMap(); // 这里利用 Google Guava 框架的 MapMaker 创建 Map 实例并赋值给 instanceConfigs
        // 初始化instance config
        initInstanceConfig(properties);

        // init socketChannel
        String socketChannel = getProperty(properties, CanalConstants.CANAL_SOCKETCHANNEL);
        if (StringUtils.isNotEmpty(socketChannel)) {
            System.setProperty(CanalConstants.CANAL_SOCKETCHANNEL, socketChannel);
        }

        // 兼容1.1.0版本的ak/sk参数名
        String accesskey = getProperty(properties, "canal.instance.rds.accesskey");
        String secretkey = getProperty(properties, "canal.instance.rds.secretkey");
        if (StringUtils.isNotEmpty(accesskey)) {
            System.setProperty(CanalConstants.CANAL_ALIYUN_ACCESSKEY, accesskey);
        }
        if (StringUtils.isNotEmpty(secretkey)) {
            System.setProperty(CanalConstants.CANAL_ALIYUN_SECRETKEY, secretkey);
        }

        // 2.准备canal server
        ip = getProperty(properties, CanalConstants.CANAL_IP);
        registerIp = getProperty(properties, CanalConstants.CANAL_REGISTER_IP);
        port = Integer.valueOf(getProperty(properties, CanalConstants.CANAL_PORT, "11111"));
        adminPort = Integer.valueOf(getProperty(properties, CanalConstants.CANAL_ADMIN_PORT, "11110"));
        embededCanalServer = CanalServerWithEmbedded.instance();
        embededCanalServer.setCanalInstanceGenerator(instanceGenerator);// 给 embededCanalServer 设置自定义的 instanceGenerator
        int metricsPort = Integer.valueOf(getProperty(properties, CanalConstants.CANAL_METRICS_PULL_PORT, "11112"));
        embededCanalServer.setMetricsPort(metricsPort);

        this.adminUser = getProperty(properties, CanalConstants.CANAL_ADMIN_USER);
        this.adminPasswd = getProperty(properties, CanalConstants.CANAL_ADMIN_PASSWD);
        embededCanalServer.setUser(getProperty(properties, CanalConstants.CANAL_USER));
        embededCanalServer.setPasswd(getProperty(properties, CanalConstants.CANAL_PASSWD));

        String canalWithoutNetty = getProperty(properties, CanalConstants.CANAL_WITHOUT_NETTY);
        if (canalWithoutNetty == null || "false".equals(canalWithoutNetty)) {
            canalServer = CanalServerWithNetty.instance();
            canalServer.setIp(ip);
            canalServer.setPort(port);
        }
        // 3.初始化zk相关代码
        // 处理下ip为空，默认使用hostIp暴露到zk中
        if (StringUtils.isEmpty(ip) && StringUtils.isEmpty(registerIp)) {
            ip = registerIp = AddressUtils.getHostIp(); // ip 为空的话在这里赋值
        }

        if (StringUtils.isEmpty(ip)) {
            ip = AddressUtils.getHostIp(); // ip 为空的话在这里赋值
        }

        if (StringUtils.isEmpty(registerIp)) {
            registerIp = ip; // 兼容以前配置
        }
        final String zkServers = getProperty(properties, CanalConstants.CANAL_ZKSERVERS);
        if (StringUtils.isNotEmpty(zkServers)) {
            zkclientx = ZkClientx.getZkClient(zkServers); // 创建 zk 实例
            // 初始化系统目录
            zkclientx.createPersistent(ZookeeperPathUtils.DESTINATION_ROOT_NODE, true); // destination列表，路径为 /otter/canal/destinations
            zkclientx.createPersistent(ZookeeperPathUtils.CANAL_CLUSTER_ROOT_NODE, true); // 整个 canal server 的集群列表，路径为 /otter/canal/cluster
        }
        // 4. CanalInstance 运行状态监控
        final ServerRunningData serverData = new ServerRunningData(registerIp + ":" + port);
        ServerRunningMonitors.setServerData(serverData); // ServerRunningMonitor 对象的容器，而 ServerRunningMonitor 用于监控 CanalInstance。
        ServerRunningMonitors.setRunningMonitors(MigrateMap.makeComputingMap((Function<String, ServerRunningMonitor>) destination -> {
            ServerRunningMonitor runningMonitor = new ServerRunningMonitor(serverData);
            runningMonitor.setDestination(destination);
            runningMonitor.setListener(new ServerRunningListener() {
                /*内部调用了embededCanalServer的start(destination)方法。
                此处需要划重点，说明每个destination对应的CanalInstance是通过embededCanalServer的start方法启动的，
                embededCanalServer负责调用instanceGenerator生成CanalInstance实例，并负责其启动。*/
                public void processActiveEnter() {
                    try {
                        MDC.put(CanalConstants.MDC_DESTINATION, String.valueOf(destination));
                        embededCanalServer.start(destination);
                        if (canalMQStarter != null) {
                            canalMQStarter.startDestination(destination);
                        }
                    } finally {
                        MDC.remove(CanalConstants.MDC_DESTINATION);
                    }
                }
                // 内部调用 embededCanalServer 的 stop(destination) 方法。与上 start 方法类似，只不过是停止 CanalInstance。
                public void processActiveExit() {
                    try {
                        MDC.put(CanalConstants.MDC_DESTINATION, String.valueOf(destination));
                        if (canalMQStarter != null) {
                            canalMQStarter.stopDestination(destination);
                        }
                        embededCanalServer.stop(destination);
                    } finally {
                        MDC.remove(CanalConstants.MDC_DESTINATION);
                    }
                }
                /*处理存在zk的情况下，在Canalinstance启动之前，在zk中创建节点。
                路径为：/otter/canal/destinations/{0}/cluster/{1}，其0会被destination替换，1会被ip:port替换。
                此方法会在processActiveEnter()之前被调用*/
                public void processStart() {
                    try {
                        if (zkclientx != null) {
                            final String path = ZookeeperPathUtils.getDestinationClusterNode(destination,
                                registerIp + ":" + port);
                            initCid(path);
                            zkclientx.subscribeStateChanges(new IZkStateListener() {

                                public void handleStateChanged(KeeperState state) throws Exception {

                                }

                                public void handleNewSession() throws Exception {
                                    initCid(path);
                                }

                                @Override
                                public void handleSessionEstablishmentError(Throwable error) throws Exception {
                                    logger.error("failed to connect to zookeeper", error);
                                }
                            });
                        }
                    } finally {
                        MDC.remove(CanalConstants.MDC_DESTINATION);
                    }
                }
                //处理存在zk的情况下，在Canalinstance停止前，释放zk节点，路径为/otter/canal/destinations/{0}/cluster/{1}，
                //其0会被destination替换，1会被ip:port替换。此方法会在processActiveExit()之前被调用
                public void processStop() {
                    try {
                        MDC.put(CanalConstants.MDC_DESTINATION, String.valueOf(destination));
                        if (zkclientx != null) {
                            final String path = ZookeeperPathUtils.getDestinationClusterNode(destination,
                                registerIp + ":" + port);
                            releaseCid(path);
                        }
                    } finally {
                        MDC.remove(CanalConstants.MDC_DESTINATION);
                    }
                }

            });
            if (zkclientx != null) {
                runningMonitor.setZkClient(zkclientx);
            }
            // 触发创建一下cid节点
            runningMonitor.init();
            return runningMonitor;
        }));

        // 5.初始化monitor机制
        autoScan = BooleanUtils.toBoolean(getProperty(properties, CanalConstants.CANAL_AUTO_SCAN));
        if (autoScan) { // 是否需要自动扫描的开关，只有当 autoScan 为 true 时，才会初始化 defaultAction 字段和 instanceConfigMonitors 字段
            defaultAction = new InstanceAction() {

                public void start(String destination) {
                    InstanceConfig config = instanceConfigs.get(destination);
                    if (config == null) {
                        // 重新读取一下instance config
                        config = parseInstanceConfig(properties, destination);
                        instanceConfigs.put(destination, config);
                    }

                    if (!embededCanalServer.isStart(destination)) {
                        // HA机制启动
                        ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(destination);
                        if (!config.getLazy() && !runningMonitor.isStart()) {
                            runningMonitor.start();
                        }
                    }

                    logger.info("auto notify start {} successful.", destination);
                }

                public void stop(String destination) {
                    // 此处的stop，代表强制退出，非HA机制，所以需要退出HA的monitor和配置信息
                    InstanceConfig config = instanceConfigs.remove(destination);
                    if (config != null) {
                        embededCanalServer.stop(destination);
                        ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(destination);
                        if (runningMonitor.isStart()) {
                            runningMonitor.stop();
                        }
                    }

                    logger.info("auto notify stop {} successful.", destination);
                }

                public void reload(String destination) {
                    // 目前任何配置变化，直接重启，简单处理
                    stop(destination);
                    start(destination);

                    logger.info("auto notify reload {} successful.", destination);
                }

                @Override
                public void release(String destination) {
                    // 此处的release，代表强制释放，主要针对HA机制释放运行，让给其他机器抢占
                    InstanceConfig config = instanceConfigs.get(destination);
                    if (config != null) {
                        ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(destination);
                        if (runningMonitor.isStart()) {
                            boolean release = runningMonitor.release();
                            if (!release) {
                                // 如果是单机模式,则直接清除配置
                                instanceConfigs.remove(destination);
                                // 停掉服务
                                runningMonitor.stop();
                                if (instanceConfigMonitors.containsKey(InstanceConfig.InstanceMode.MANAGER)) {
                                    ManagerInstanceConfigMonitor monitor = (ManagerInstanceConfigMonitor) instanceConfigMonitors.get(InstanceConfig.InstanceMode.MANAGER);
                                    Map<String, InstanceAction> instanceActions = monitor.getActions();
                                    if (instanceActions.containsKey(destination)) {
                                        // 清除内存中的autoScan cache
                                        monitor.release(destination);
                                    }
                                }
                            }
                        }
                    }

                    logger.info("auto notify release {} successful.", destination);
                }
            };

            instanceConfigMonitors = MigrateMap.makeComputingMap(mode -> {
                int scanInterval = Integer.valueOf(getProperty(properties,
                    CanalConstants.CANAL_AUTO_SCAN_INTERVAL,
                    "5"));

                if (mode.isSpring()) { // 如果加载方式是 spring，返回 SpringInstanceConfigMonitor
                    SpringInstanceConfigMonitor monitor = new SpringInstanceConfigMonitor();
                    monitor.setScanIntervalInSecond(scanInterval);
                    monitor.setDefaultAction(defaultAction);
                    // 设置conf目录，默认是user.dir + conf目录组成
                    String rootDir = getProperty(properties, CanalConstants.CANAL_CONF_DIR);
                    if (StringUtils.isEmpty(rootDir)) {
                        rootDir = "../conf";
                    }

                    if (StringUtils.equals("otter-canal", System.getProperty("appName"))) {
                        monitor.setRootConf(rootDir);
                    } else {
                        // eclipse debug模式
                        monitor.setRootConf("src/main/resources/");
                    }
                    return monitor;
                } else if (mode.isManager()) { // 如果加载方式是 manager，返回 ManagerInstanceConfigMonitor
                    ManagerInstanceConfigMonitor monitor = new ManagerInstanceConfigMonitor();
                    monitor.setScanIntervalInSecond(scanInterval);
                    monitor.setDefaultAction(defaultAction);
                    String managerAddress = getProperty(properties, CanalConstants.CANAL_ADMIN_MANAGER);
                    monitor.setConfigClient(getManagerClient(managerAddress));
                    return monitor;
                } else {
                    throw new UnsupportedOperationException("unknow mode :" + mode + " for monitor");
                }
            });
        }
    }
    // canal instance 的全局配置
    private InstanceConfig initGlobalConfig(Properties properties) {
        String adminManagerAddress = getProperty(properties, CanalConstants.CANAL_ADMIN_MANAGER);
        InstanceConfig globalConfig = new InstanceConfig();
        String modeStr = getProperty(properties, CanalConstants.getInstanceModeKey(CanalConstants.GLOBAL_NAME));  // 读取 canal.instance.global.mode
        if (StringUtils.isNotEmpty(adminManagerAddress)) {
            // 如果指定了manager地址,则强制适用manager
            globalConfig.setMode(InstanceMode.MANAGER);
        } else if (StringUtils.isNotEmpty(modeStr)) {
            globalConfig.setMode(InstanceMode.valueOf(StringUtils.upperCase(modeStr))); //将 modelStr 转成枚举 InstanceMode，这是一个枚举类，只有2个取值，SPRING/MANAGER，对应两种配置方式
        }
        // 读取 canal.instance.global.lazy
        String lazyStr = getProperty(properties, CanalConstants.getInstancLazyKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(lazyStr)) {
            globalConfig.setLazy(Boolean.valueOf(lazyStr));
        }
        // 读取 canal.instance.global.manager.address
        String managerAddress = getProperty(properties,
            CanalConstants.getInstanceManagerAddressKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(managerAddress)) {
            if (StringUtils.equals(managerAddress, "${canal.admin.manager}")) {
                managerAddress = adminManagerAddress;
            }

            globalConfig.setManagerAddress(managerAddress);
        }
        // 读取 canal.instance.global.spring.xml
        String springXml = getProperty(properties, CanalConstants.getInstancSpringXmlKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(springXml)) {
            globalConfig.setSpringXml(springXml);
        }
        // 初始化匿名内部类 instanceGenerator
        instanceGenerator = destination -> {
            InstanceConfig config = instanceConfigs.get(destination); // 1.根据 destination从instanceConfigs 获取对应的 InstanceConfig 对象
            if (config == null) {
                throw new CanalServerException("can't find destination:" + destination);
            }

            if (config.getMode().isManager()) { // 2.如果 destination 对应的 InstanceConfig 的 mode 是 manager 方式，使用 ManagerCanalInstanceGenerator
                PlainCanalInstanceGenerator instanceGenerator = new PlainCanalInstanceGenerator(properties);
                instanceGenerator.setCanalConfigClient(managerClients.get(config.getManagerAddress()));
                instanceGenerator.setSpringXml(config.getSpringXml());
                return instanceGenerator.generate(destination);
            } else if (config.getMode().isSpring()) { // 3.如果 destination 对应的 InstanceConfig 的 mode 是 spring 方式，使用 SpringCanalInstanceGenerator
                SpringCanalInstanceGenerator instanceGenerator = new SpringCanalInstanceGenerator();
                instanceGenerator.setSpringXml(config.getSpringXml());
                return instanceGenerator.generate(destination);
            } else {
                throw new UnsupportedOperationException("unknow mode :" + config.getMode());
            }

        };

        return globalConfig;
    }

    private PlainCanalConfigClient getManagerClient(String managerAddress) {
        return new PlainCanalConfigClient(managerAddress, this.adminUser, this.adminPasswd, this.registerIp, adminPort);
    }
    // instanceConfigs 字段通过 initInstanceConfig 方法进行初始化
    private void initInstanceConfig(Properties properties) {
        String destinationStr = getProperty(properties, CanalConstants.CANAL_DESTINATIONS); // 读取配置项 canal.destinations
        String[] destinations = StringUtils.split(destinationStr, CanalConstants.CANAL_DESTINATION_SPLIT); // 以 "," 分割canal.destinations，得到一个数组形式的 destination
        // 遍历 destination
        for (String destination : destinations) {
            InstanceConfig config = parseInstanceConfig(properties, destination); // 为每一个 destination 生成一个 InstanceConfig 实例
            InstanceConfig oldConfig = instanceConfigs.put(destination, config); // 将 destination 对应的 InstanceConfig 放入 instanceConfigs 中

            if (oldConfig != null) {
                logger.warn("destination:{} old config:{} has replace by new config:{}", destination, oldConfig, config);
            }
        }
    }

    private InstanceConfig parseInstanceConfig(Properties properties, String destination) {
        String adminManagerAddress = getProperty(properties, CanalConstants.CANAL_ADMIN_MANAGER);
        InstanceConfig config = new InstanceConfig(globalInstanceConfig); // 每个 destination 对应的 InstanceConfig 都引用了全局的 globalInstanceConfig
        String modeStr = getProperty(properties, CanalConstants.getInstanceModeKey(destination));
        if (StringUtils.isNotEmpty(adminManagerAddress)) {
            // 如果指定了manager地址,则强制适用manager
            config.setMode(InstanceMode.MANAGER);
        } else if (StringUtils.isNotEmpty(modeStr)) {
            config.setMode(InstanceMode.valueOf(StringUtils.upperCase(modeStr)));
        }

        String lazyStr = getProperty(properties, CanalConstants.getInstancLazyKey(destination));
        if (!StringUtils.isEmpty(lazyStr)) {
            config.setLazy(Boolean.valueOf(lazyStr));
        }

        if (config.getMode().isManager()) {
            String managerAddress = getProperty(properties, CanalConstants.getInstanceManagerAddressKey(destination));
            if (StringUtils.isNotEmpty(managerAddress)) {
                if (StringUtils.equals(managerAddress, "${canal.admin.manager}")) {
                    managerAddress = adminManagerAddress;
                }
                config.setManagerAddress(managerAddress);
            }
        }

        String springXml = getProperty(properties, CanalConstants.getInstancSpringXmlKey(destination));
        if (StringUtils.isNotEmpty(springXml)) {
            config.setSpringXml(springXml);
        }

        return config;
    }

    public static String getProperty(Properties properties, String key, String defaultValue) {
        String value = getProperty(properties, key);
        if (StringUtils.isEmpty(value)) {
            return defaultValue;
        } else {
            return value;
        }
    }

    public static String getProperty(Properties properties, String key) {
        key = StringUtils.trim(key);
        String value = System.getProperty(key);

        if (value == null) {
            value = System.getenv(key);
        }

        if (value == null) {
            value = properties.getProperty(key);
        }

        return StringUtils.trim(value);
    }
    // ServerRunningMonitor 的 start 方法，是在 CanalController 中的 start 方法中被调用的，CanalController 中的 start 方法是在 CanalLauncher 中被调用的
    public void start() throws Throwable {
        logger.info("## start the canal server[{}({}):{}]", ip, registerIp, port);
        // 创建整个canal的工作节点
        final String path = ZookeeperPathUtils.getCanalClusterNode(registerIp + ":" + port); // 创建整个canal的工作节点 :/otter/canal/cluster/{0}，如 /otter/canal/cluster/192.168.1.18:11111
        initCid(path);
        if (zkclientx != null) {
            this.zkclientx.subscribeStateChanges(new IZkStateListener() {

                public void handleStateChanged(KeeperState state) throws Exception {

                }

                public void handleNewSession() throws Exception {
                    initCid(path);
                }

                @Override
                public void handleSessionEstablishmentError(Throwable error) throws Exception {
                    logger.error("failed to connect to zookeeper", error);
                }
            });
        }
        // 优先启动embeded服务
        embededCanalServer.start();
        // 尝试启动一下非lazy状态的通道，启动不是 lazy 模式的 CanalInstance，通过迭代 instanceConfigs，根据 destination 获取对应的 ServerRunningMonitor，然后逐一启动
        for (Map.Entry<String, InstanceConfig> entry : instanceConfigs.entrySet()) {
            final String destination = entry.getKey();
            InstanceConfig config = entry.getValue();
            // 创建destination的工作节点
            if (!embededCanalServer.isStart(destination)) { // 如果 destination 对应的 CanalInstance 没有启动，则进行启动
                // HA机制启动
                ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(destination);
                if (!config.getLazy() && !runningMonitor.isStart()) { // 如果不是 lazy，lazy 模式需要等到第一次有客户端请求才会启动
                    runningMonitor.start();
                }
            }

            if (autoScan) {
                instanceConfigMonitors.get(config.getMode()).register(destination, defaultAction);
            }
        }

        if (autoScan) {  // 启动配置文件自动检测机制
            instanceConfigMonitors.get(globalInstanceConfig.getMode()).start();
            for (InstanceConfigMonitor monitor : instanceConfigMonitors.values()) {
                if (!monitor.isStart()) {
                    monitor.start(); // 启动monitor
                }
            }
        }

        // 启动 CanalServerWithNetty 网络接口，监听客户端请求
        if (canalServer != null) {
            canalServer.start();
        }
    }

    public void stop() throws Throwable {

        if (canalServer != null) {
            canalServer.stop();
        }

        if (autoScan) {
            for (InstanceConfigMonitor monitor : instanceConfigMonitors.values()) {
                if (monitor.isStart()) {
                    monitor.stop();
                }
            }
        }

        for (ServerRunningMonitor runningMonitor : ServerRunningMonitors.getRunningMonitors().values()) {
            if (runningMonitor.isStart()) {
                runningMonitor.stop();
            }
        }

        // 释放canal的工作节点
        releaseCid(ZookeeperPathUtils.getCanalClusterNode(registerIp + ":" + port));
        logger.info("## stop the canal server[{}({}):{}]", ip, registerIp, port);

        if (zkclientx != null) {
            zkclientx.close();
        }

        // 关闭时清理缓存
        if (instanceConfigs != null) {
            instanceConfigs.clear();
        }
        if (managerClients != null) {
            managerClients.clear();
        }
        if (instanceConfigMonitors != null) {
            instanceConfigMonitors.clear();
        }

        ZkClientx.clearClients();
    }

    private void initCid(String path) {
        // logger.info("## init the canalId = {}", cid);
        // 初始化系统目录
        if (zkclientx != null) {
            try {
                zkclientx.createEphemeral(path);
            } catch (ZkNoNodeException e) {
                // 如果父目录不存在，则创建
                String parentDir = path.substring(0, path.lastIndexOf('/'));
                zkclientx.createPersistent(parentDir, true);
                zkclientx.createEphemeral(path);
            } catch (ZkNodeExistsException e) {
                // ignore
                // 因为第一次启动时创建了cid,但在stop/start的时可能会关闭和新建,允许出现NodeExists问题s
            }

        }
    }

    private void releaseCid(String path) {
        // logger.info("## release the canalId = {}", cid);
        // 初始化系统目录
        if (zkclientx != null) {
            zkclientx.delete(path);
        }
    }

    public CanalMQStarter getCanalMQStarter() {
        return canalMQStarter;
    }

    public void setCanalMQStarter(CanalMQStarter canalMQStarter) {
        this.canalMQStarter = canalMQStarter;
    }

    public Map<InstanceMode, InstanceConfigMonitor> getInstanceConfigMonitors() {
        return instanceConfigMonitors;
    }

    public Map<String, InstanceConfig> getInstanceConfigs() {
        return instanceConfigs;
    }

}
