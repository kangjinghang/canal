package com.alibaba.otter.canal.store.model;

/**
 * 批处理模式。表示 canal 内存 store 中数据缓存模式
 * 
 * @author jianghang 2013-3-18 上午11:51:15
 * @version 1.0.3
 */
public enum BatchMode {
    // 根据 buffer.size 进行限制，只限制记录的数量。这种方式有一些潜在的问题，举个极端例子，假设每个 event 有 1M，那么16384个这种event占用内存要达到 16G 左右，基本上肯定会造成内存溢出(超大内存的物理机除外)。
    /** 对象数量 */
    ITEMSIZE,
    // 根据buffer.size * buffer.memunit 的大小，限制缓存记录占用的总内存大小。指定为这种模式时，意味着默认缓存的event占用的总内存不能超过16384*1024=16M。因为通常我们在一个服务器上会部署多个instance，每个instance的store模块都会占用16M，因此只要instance的数量合适，也就不会浪费内存了。部分读者可能会担心，这是否限制了一个event的最大大小为16M，实际上是没有这个限制的。因为canal在Put一个新的event时，只会判断队列中已有的event占用的内存是否超过16M，如果没有，新的event不论大小是多少，总是可以放入的(canal的内存计算实际上是不精确的)，之后的event再要放入时，如果这个超过16M的event没有被消费，则需要进行等待。
    /** 内存大小 */
    MEMSIZE;

    public boolean isItemSize() {
        return this == BatchMode.ITEMSIZE;
    }

    public boolean isMemSize() {
        return this == BatchMode.MEMSIZE;
    }
}
