package com.alibaba.otter.canal.deployer;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.canal.common.utils.CanalToStringStyle;

/**
 * 启动的相关配置
 * 相关 get 方法在执行时，会按照以下逻辑进行判断：如果没有自身没有这个配置，则返回全局配置，如果有，则返回自身的配置。通过这种方式实现对全局配置的覆盖。
 * @author jianghang 2012-11-8 下午02:50:54
 * @version 1.0.0
 */
public class InstanceConfig {

    private InstanceConfig globalConfig;
    private InstanceMode   mode;
    private Boolean        lazy;
    private String         managerAddress;
    private String         springXml;

    public InstanceConfig(){

    }

    public InstanceConfig(InstanceConfig globalConfig){
        this.globalConfig = globalConfig;
    }

    public static enum InstanceMode {
        SPRING, MANAGER;

        public boolean isSpring() {
            return this == InstanceMode.SPRING;
        }

        public boolean isManager() {
            return this == InstanceMode.MANAGER;
        }
    }

    public Boolean getLazy() {
        if (lazy == null && globalConfig != null) {
            return globalConfig.getLazy();
        } else {
            return lazy;
        }
    }

    public void setLazy(Boolean lazy) {
        this.lazy = lazy;
    }

    public InstanceMode getMode() {
        if (mode == null && globalConfig != null) {
            return globalConfig.getMode();
        } else {
            return mode;
        }
    }

    public void setMode(InstanceMode mode) {
        this.mode = mode;
    }

    public String getManagerAddress() {
        if (managerAddress == null && globalConfig != null) {
            return globalConfig.getManagerAddress();
        } else {
            return managerAddress;
        }
    }

    public void setManagerAddress(String managerAddress) {
        this.managerAddress = managerAddress;
    }

    public String getSpringXml() {
        if (springXml == null && globalConfig != null) {
            return globalConfig.getSpringXml();
        } else {
            return springXml;
        }
    }

    public void setSpringXml(String springXml) {
        this.springXml = springXml;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }

}
