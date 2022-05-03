package com.alibaba.otter.canal.store.memory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.alibaba.otter.canal.store.AbstractCanalStoreScavenge;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.CanalStoreException;
import com.alibaba.otter.canal.store.CanalStoreScavenge;
import com.alibaba.otter.canal.store.helper.CanalEventUtils;
import com.alibaba.otter.canal.store.model.BatchMode;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.Events;

/**
 * 基于内存buffer构建内存memory store
 * 
 * <pre>
 * 变更记录：
 * 1. 新增BatchMode类型，支持按内存大小获取批次数据，内存大小更加可控.
 *   a. put操作，会首先根据bufferSize进行控制，然后再进行bufferSize * bufferMemUnit进行控制. 因存储的内容是以Event，如果纯依赖于memsize进行控制，会导致RingBuffer出现动态伸缩
 * </pre>
 * 
 * @author jianghang 2012-6-20 上午09:46:31
 * @version 1.0.0
 */
public class MemoryEventStoreWithBuffer extends AbstractCanalStoreScavenge implements CanalEventStore<Event>, CanalStoreScavenge {

    private static final long INIT_SEQUENCE = -1;
    private int               bufferSize    = 16 * 1024; // 表示 RingBuffer 队列的最大容量，也就是可缓存的 binlog 事件的最大记录数
    private int               bufferMemUnit = 1024;                                      // memsize的单位，默认为1kb大小。表示RingBuffer使用的内存单元，和canal.instance.memory.buffer.size组合决定最终的内存使用大小。需要注意的是，这个配置项仅仅是用于计算占用总内存，并不是限制每个event最大为1kb。
    private int               indexMask; // 用于对 putSequence、getSequence、ackSequence 进行取余操作
    private Event[]           entries;
    // 当前可消费的event数量 = putSequence - getSequence。当前队列的大小(即队列中还有多少事件等待消费) = putSequence - ackSequence
    // 记录下put/get/ack操作的三个下标。ackSequence <= getSequence <= putSequence
    private AtomicLong        putSequence   = new AtomicLong(INIT_SEQUENCE);             // 代表当前put操作最后一次写操作发生的位置。每放入一个数据putSequence +1，可表示存储数据存储的总数量
    private AtomicLong        getSequence   = new AtomicLong(INIT_SEQUENCE);             // 代表当前get操作读取的最后一条的位置。每获取一个数据getSequence +1，可表示数据订阅获取的最后一次提取位置
    private AtomicLong        ackSequence   = new AtomicLong(INIT_SEQUENCE);             // 代表当前ack操作的最后一条的位置。每确认一个数据ackSequence + 1，可表示数据最后一次消费成功位置

    // 记录下put/get/ack操作的三个memsize大小。例如每put一个event，putMemSize就要增加这个event占用的内存大小；get和ack操作也是类似。这三个变量，都是在batchMode指定为MEMSIZE的情况下，才会发生作用。
    private AtomicLong        putMemSize    = new AtomicLong(0);
    private AtomicLong        getMemSize    = new AtomicLong(0);
    private AtomicLong        ackMemSize    = new AtomicLong(0);

    // 记录下put/get/ack操作的三个execTime
    private AtomicLong        putExecTime   = new AtomicLong(System.currentTimeMillis());
    private AtomicLong        getExecTime   = new AtomicLong(System.currentTimeMillis());
    private AtomicLong        ackExecTime   = new AtomicLong(System.currentTimeMillis());

    // 记录下put/get/ack操作的三个table rows
    private AtomicLong        putTableRows  = new AtomicLong(0);
    private AtomicLong        getTableRows  = new AtomicLong(0);
    private AtomicLong        ackTableRows  = new AtomicLong(0);

    // 阻塞put/get操作控制信号
    private ReentrantLock     lock          = new ReentrantLock();
    private Condition         notFull       = lock.newCondition();
    private Condition         notEmpty      = lock.newCondition();

    private BatchMode         batchMode     = BatchMode.ITEMSIZE;                        // 默认为内存大小模式
    private boolean           ddlIsolation  = false;
    private boolean           raw           = true;                                      // 针对entry是否开启raw模式

    public MemoryEventStoreWithBuffer(){

    }

    public MemoryEventStoreWithBuffer(BatchMode batchMode){
        this.batchMode = batchMode;
    }
    // 初始化一下 Event[] 数组
    public void start() throws CanalStoreException {
        super.start();
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }

        indexMask = bufferSize - 1;
        entries = new Event[bufferSize];
    }

    public void stop() throws CanalStoreException {
        super.stop();

        cleanAll();
    }

    public void put(List<Event> data) throws InterruptedException, CanalStoreException {
        if (data == null || data.isEmpty()) {
            return;
        }

        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            try {
                while (!checkFreeSlotAt(putSequence.get() + data.size())) { // 检查是否有空位
                    notFull.await(); // wait until not full
                }
            } catch (InterruptedException ie) {
                notFull.signal(); // propagate to non-interrupted thread
                throw ie;
            }
            doPut(data);
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean put(List<Event> data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        if (data == null || data.isEmpty()) { // 1. 如果需要插入的 List 为空，直接返回 true
            return true;
        }

        long nanos = unit.toNanos(timeout); // 2. 获得超时时间，并通过加锁进行 put 操作
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            for (;;) { // 这是一个死循环，执行到下面任意一个 return 或者抛出异常是时才会停止
                if (checkFreeSlotAt(putSequence.get() + data.size())) { // 3. 检查是否满足插入条件，如果满足，进入到3.1，否则进入到3.2
                    doPut(data); // 3.1 如果满足条件，调用 doPut 方法进行真正的插入
                    return true;
                }
                if (nanos <= 0) {  // 3.2 判断是否已经超时，如果超时，则不执行插入操作，直接返回 false
                    return false;
                }
                // 3.3 如果还没有超时，调用 notFull.awaitNanos 进行等待，需要其他线程调用 notFull.signal() 方法唤醒。
                try { // 唤醒是在 ack 操作中进行的，ack 操作会删除已经消费成功的 event，此时队列有了空间，因此可以唤醒
                    nanos = notFull.awaitNanos(nanos); // 被唤醒后，因为这是一个死循环，所以循环中的代码会重复执行。当插入条件满足时，调用 doPut 方法插入，然后返回
                } catch (InterruptedException ie) { // 3.4 如果一直等待到超时，都没有可用空间可以插入，notFull.awaitNanos 会抛出 InterruptedException
                    notFull.signal(); // propagate to non-interrupted thread // 3.5 超时之后，唤醒一个其他执行 put 操作且未被中断的线程(不明白是为了干啥)
                    throw ie;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean tryPut(List<Event> data) throws CanalStoreException {
        if (data == null || data.isEmpty()) {
            return true;
        }

        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (!checkFreeSlotAt(putSequence.get() + data.size())) {
                return false;
            } else {
                doPut(data);
                return true;
            }
        } finally {
            lock.unlock();
        }
    }
    // 添加数据。event parser 模块拉取到 binlog 后，并经过 event sink 模块过滤，最终就通过 Put 操作存储到了队列中。
    public void put(Event data) throws InterruptedException, CanalStoreException {
        put(Arrays.asList(data));
    }

    public boolean put(Event data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        return put(Arrays.asList(data), timeout, unit);
    }

    public boolean tryPut(Event data) throws CanalStoreException {
        return tryPut(Arrays.asList(data));
    }

    /**
     * 执行具体的put操作
     */
    private void doPut(List<Event> data) {
        long current = putSequence.get(); // 1. 将新插入的 event 数据赋值到 Event[] 数组的正确位置上
        long end = current + data.size(); // 1.1 获得 putSequence 的当前值 current，和插入数据后的 putSequence 结束值 end

        // 先写数据，再更新对应的cursor,并发度高的情况，putSequence会被get请求可见，拿出了ringbuffer中的老的Entry值
        for (long next = current + 1; next <= end; next++) { // 1.2 循环需要插入的数据，从 current + 1 位置开始，到 end 位置结束
            entries[getIndex(next)] = data.get((int) (next - current - 1)); // 1.3 通过 getIndex 方法对 next 变量转换成正确的位置，设置到 Event[] 数组中
        }
        // 2. 直接设置 putSequence 为 end 值，相当于完成 event 记录数的累加
        putSequence.set(end);

        // 记录一下gets memsize信息，方便快速检索
        if (batchMode.isMemSize()) { // 3. 累加新插入的 event 的大小到 putMemSize 上
            long size = 0; // 用于记录本次插入的 event 记录的大小
            for (Event event : data) {
                size += calculateSize(event);  // 通过 calculateSize 方法计算每个 event 的大小，并累加到 size 变量上
            }

            putMemSize.getAndAdd(size); // 将 size 变量的值，添加到当前 putMemSize
        }
        profiling(data, OP.PUT);
        // tell other threads that store is not empty
        notEmpty.signal(); // 4. 调用 notEmpty.signal() 方法，通知队列中有数据了，如果之前有 client 获取数据处于阻塞状态，将会被唤醒
    }
    // 获取数据。canal client 连接到 canal server 后，最终获取到的 binlog 都是从这个队列中取得。
    public Events<Event> get(Position start, int batchSize) throws InterruptedException, CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            try {
                while (!checkUnGetSlotAt((LogPosition) start, batchSize))
                    notEmpty.await();
            } catch (InterruptedException ie) {
                notEmpty.signal(); // propagate to non-interrupted thread
                throw ie;
            }

            return doGet(start, batchSize);
        } finally {
            lock.unlock();
        }
    }

    public Events<Event> get(Position start, int batchSize, long timeout, TimeUnit unit) throws InterruptedException,
                                                                                        CanalStoreException {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            for (;;) {
                if (checkUnGetSlotAt((LogPosition) start, batchSize)) { // 检查是否有足够的event可供获取
                    return doGet(start, batchSize);
                }

                if (nanos <= 0) {
                    // 如果时间到了，有多少取多少
                    return doGet(start, batchSize);
                }

                try {
                    nanos = notEmpty.awaitNanos(nanos);
                } catch (InterruptedException ie) {
                    notEmpty.signal(); // propagate to non-interrupted thread
                    throw ie;
                }

            }
        } finally {
            lock.unlock();
        }
    }

    public Events<Event> tryGet(Position start, int batchSize) throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return doGet(start, batchSize);
        } finally {
            lock.unlock();
        }
    }

    private Events<Event> doGet(Position start, int batchSize) throws CanalStoreException {
        LogPosition startPosition = (LogPosition) start;
        // 1. 确定从哪个位置开始获取数。获得当前的 get 位置
        long current = getSequence.get();
        long maxAbleSequence = putSequence.get(); // 获得当前的 put 位置
        long next = current; // 要获取的第一个 Event 的位置，一开始等于当前 get 位置
        long end = current; // 要获取的最后一个event的位置，一开始也是当前get位置，每获取一个event，end值加1，最大为current+batchSize。因为可能进行ddl隔离，因此可能没有获取到batchSize个event就返回了，此时end值就会小于current+batchSize
        // 如果startPosition为null，说明是第一次订阅，默认+1处理，因为getSequence的值是从-1开始的。 如果startPosition不为null，需要包含一下start位置，防止丢失第一条记录
        if (startPosition == null || !startPosition.getPostion().isIncluded()) { // 第一次订阅之后，需要包含一下start位置，防止丢失第一条记录
            next = next + 1;
        }
        // 如果没有数据，直接返回一个空列表
        if (current >= maxAbleSequence) {
            return new Events<>();
        }
        // 2. 如果有数据，根据 batchMode 是 ITEMSIZE 或 MEMSIZE 选择不同的处理方式
        Events<Event> result = new Events<>();
        List<Event> entrys = result.getEvents(); // 维护要返回的 Event 列表
        long memsize = 0;
        if (batchMode.isItemSize()) {  // 2.1 如果 batchMode 是 ITEMSIZE
            end = (next + batchSize - 1) < maxAbleSequence ? (next + batchSize - 1) : maxAbleSequence;
            // 提取数据并返回
            for (; next <= end; next++) { // 2.1.1 循环从开始位置(next)到结束位置(end)，每次循环 next + 1
                Event event = entries[getIndex(next)]; // 2.1.2 获取指定位置上的事件
                if (ddlIsolation && isDdl(event.getEventType())) {  // 2.1.3 果是当前事件是 DDL 事件，且开启了 DDL 隔离，本次事件处理完后，即结束循环 (if语句最后是一行是break)
                    // 如果是ddl隔离，直接返回
                    if (entrys.size() == 0) { // 2.1.4 因为 DDL 事件需要单独返回，因此需要判断 entrys 中是否应添加了其他事件
                        entrys.add(event);// 如果没有DML事件，加入当前的DDL事件
                        end = next; // 更新end为当前
                    } else {
                        // 如果之前已经有DML事件，直接返回了，因为不包含当前next这记录，需要回退一个位置
                        end = next - 1; // next-1一定大于current，不需要判断
                    }
                    break;
                } else {
                    entrys.add(event); // 如果没有开启 DDL 隔离，直接将事件加入到 entrys 中
                }
            }
        } else { // 2.2 如果 batchMode 是 MEMSIZE
            long maxMemSize = batchSize * bufferMemUnit;  // 2.2.1 计算本次要获取的 event 占用最大字节数
            for (; memsize <= maxMemSize && next <= maxAbleSequence; next++) {  // 2.2.2 memsize 从 0 开始，当 memsize 小于 maxMemSize 且 next 未超过 maxAbleSequence 时，可以进行循环
                // 永远保证可以取出第一条的记录，避免死锁
                Event event = entries[getIndex(next)];  // 2.2.3 获取指定位置上的 Event
                if (ddlIsolation && isDdl(event.getEventType())) { // 2.2.4 果是当前事件是 DDL 事件，且开启了 DDL 隔离，本次事件处理完后，即结束循环 (if语句最后是一行是break)
                    // 如果是ddl隔离，直接返回
                    if (entrys.size() == 0) {
                        entrys.add(event);// 如果没有DML事件，加入当前的DDL事件
                        end = next; // 更新end为当前
                    } else {
                        // 如果之前已经有DML事件，直接返回了，因为不包含当前next这记录，需要回退一个位置
                        end = next - 1; // next-1一定大于current，不需要判断
                    }
                    break;
                } else {
                    entrys.add(event); // 如果没有开启 DDL 隔离，直接将事件加入到 entrys 中
                    memsize += calculateSize(event); // 并将当前添加的 event 占用字节数累加到 memsize 变量上
                    end = next;// 记录end位点
                }
            }

        }
        // 3. 构造 PositionRange，表示本次获取的 Event 的开始和结束位置
        PositionRange<LogPosition> range = new PositionRange<>();
        result.setPositionRange(range);
        // 3.1 把 entrys 列表中的第一个 event 的位置，当做 PositionRange 的开始位置
        range.setStart(CanalEventUtils.createPosition(entrys.get(0)));
        range.setEnd(CanalEventUtils.createPosition(entrys.get(result.getEvents().size() - 1))); // 3.2 把 entrys 列表中的最后一个 event 的位置，当做 PositionRange 的结束位置
        range.setEndSeq(end);
        // 记录一下是否存在可以被ack的点
        // 4. 记录一下是否存在可以被 ack 的点，逆序迭代获取到的 Event 列表
        for (int i = entrys.size() - 1; i >= 0; i--) {
            Event event = entrys.get(i); // 4.1.1 如果是事务开始/事务结束/或者dll事件，
            // GTID模式,ack的位点必须是事务结尾,因为下一次订阅的时候mysql会发送这个gtid之后的next,如果在事务头就记录了会丢这最后一个事务
            if ((CanalEntry.EntryType.TRANSACTIONBEGIN == event.getEntryType() && StringUtils.isEmpty(event.getGtid()))
                || CanalEntry.EntryType.TRANSACTIONEND == event.getEntryType() || isDdl(event.getEventType())) {
                // 将事务头/尾设置可被为ack的点
                range.setAck(CanalEventUtils.createPosition(event)); // 4.1.2 将其设置为可被ack的点，并跳出循环
                break;
            } //4.1.3 如果没有这三种类型事件，意味着没有可被ack的点
        }
        // 5. 累加 getMemSize 值，getMemSize 值
        if (getSequence.compareAndSet(current, end)) { // 5.1 通过AtomLong的compareAndSet尝试增加getSequence值。如果成功，累加getMemSize
            getMemSize.addAndGet(memsize);
            notFull.signal(); // 如果之前有put操作因为队列满了而被阻塞，这里发送信号，通知队列已经有空位置
            profiling(result.getEvents(), OP.GET);
            return result;
        } else { // 如果失败，直接返回空事件列表
            return new Events<>();
        }
    }
    // 第一条数据通过ackSequence当前值对应的Event来确定，因为更早的Event在ack后都已经被删除了
    public LogPosition getFirstPosition() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            long firstSeqeuence = ackSequence.get();
            if (firstSeqeuence == INIT_SEQUENCE && firstSeqeuence < putSequence.get()) { // 1. 没有 ack 过数据，且队列中有数据
                // 没有ack过数据，那么ack为初始值-1，又因为队列中有数据，因此ack+1,即返回队列中第一条数据的位置
                Event event = entries[getIndex(firstSeqeuence + 1)]; // 最后一次ack为-1，需要移动到下一条,included
                                                                     // = false
                return CanalEventUtils.createPosition(event, false);
            } else if (firstSeqeuence > INIT_SEQUENCE && firstSeqeuence < putSequence.get()) {  // 2. 已经 ack 过数据，但是未追上 put 操作
                // ack未追上put操作
                Event event = entries[getIndex(firstSeqeuence)]; // 最后一次ack的位置数据,需要移动到下一条,included
                // = false
                return CanalEventUtils.createPosition(event, false);
            } else if (firstSeqeuence > INIT_SEQUENCE && firstSeqeuence == putSequence.get()) {  // 3. 已经 ack 过数据，且已经追上 put 操作，说明队列中所有数据都被消费完了
                // 已经追上，store中没有数据
                Event event = entries[getIndex(firstSeqeuence)]; // 最后一次ack的位置数据，和last为同一条，included
                                                                 // = false
                return CanalEventUtils.createPosition(event, false);
            } else {
                // 4. 没有任何数据，返回null
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    public LogPosition getLatestPosition() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            long latestSequence = putSequence.get();
            if (latestSequence > INIT_SEQUENCE && latestSequence != ackSequence.get()) {
                Event event = entries[(int) putSequence.get() & indexMask]; // 最后一次写入的数据，最后一条未消费的数据
                return CanalEventUtils.createPosition(event, true);
            } else if (latestSequence > INIT_SEQUENCE && latestSequence == ackSequence.get()) {
                // ack已经追上了put操作
                Event event = entries[(int) putSequence.get() & indexMask]; // 最后一次写入的数据，included
                                                                            // =
                                                                            // false
                return CanalEventUtils.createPosition(event, false);
            } else {
                // 没有任何数据
                return null;
            }
        } finally {
            lock.unlock();
        }
    }
    // 确认消费成功。canal client 获取到 binlog 事件消费后，需要进行 Ack。你可以认为 Ack 操作实际上就是将消费成功的事件从队列中删除，如果一直不 Ack 的话，队列满了之后，Put 操作就无法添加新的数据了。
    public void ack(Position position) throws CanalStoreException {
        cleanUntil(position, -1L);
    }

    public void ack(Position position, Long seqId) throws CanalStoreException {
        cleanUntil(position, seqId);
    }

    @Override
    public void cleanUntil(Position position) throws CanalStoreException {
        cleanUntil(position, -1L);
    }
    // 在 stop 时，cleanAll 方法会被执行。而每次 ack 时，cleanUntil 方法会被执行
    public void cleanUntil(Position position, Long seqId) throws CanalStoreException { // postion表示要ack的配置
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            long sequence = ackSequence.get(); // 获得当前 ack 值
            long maxSequence = getSequence.get(); // 获得当前 get 值

            boolean hasMatch = false;
            long memsize = 0;
            // ack没有list，但有已存在的foreach，还是节省一下list的开销
            long localExecTime = 0L;
            int deltaRows = 0;
            if (seqId > 0) {
                maxSequence = seqId;
            }
            for (long next = sequence + 1; next <= maxSequence; next++) { // 迭代所有未被ack的event，从中找出与需要ack的position相同位置的event，清空这个event之前的所有数据。一旦找到这个event，循环结束。
                Event event = entries[getIndex(next)]; // 获得要 ack 的 event
                if (localExecTime == 0 && event.getExecuteTime() > 0) {
                    localExecTime = event.getExecuteTime();
                }
                deltaRows += event.getRowsCount();
                memsize += calculateSize(event); // 计算当前要 ack 的 event 占用字节数
                if ((seqId < 0 || next == seqId) && CanalEventUtils.checkPosition(event, (LogPosition) position)) { // 找到对应的 position，更新 ack seq
                    // 找到对应的position，更新ack seq
                    hasMatch = true;

                    if (batchMode.isMemSize()) { // 如果 batchMode 是 MEMSIZE
                        ackMemSize.addAndGet(memsize); // 累加 ackMemSize
                        // 尝试清空buffer中的内存，将ack之前的内存全部释放掉
                        for (long index = sequence + 1; index < next; index++) { // 尝试清空 buffer 中的内存，将 ack 之前的内存全部释放掉
                            entries[getIndex(index)] = null;// 设置为null
                        }

                        // 考虑getFirstPosition/getLastPosition会获取最后一次ack的position信息
                        // ack清理的时候只处理entry=null，释放内存
                        Event lastEvent = entries[getIndex(next)];
                        lastEvent.setEntry(null);
                        lastEvent.setRawEntry(null);
                    }
                    // 累加 ack 值
                    if (ackSequence.compareAndSet(sequence, next)) {// 避免并发ack
                        notFull.signal(); // 如果之前存在 put 操作因为队列满了而被阻塞，通知其队列有了新空间
                        ackTableRows.addAndGet(deltaRows);
                        if (localExecTime > 0) {
                            ackExecTime.lazySet(localExecTime);
                        }
                        return;
                    }
                }
            }
            if (!hasMatch) {// 找不到对应需要ack的position
                throw new CanalStoreException("no match ack position" + position.toString());
            }
        } finally {
            lock.unlock();
        }
    }
    // 所谓rollback，就是client已经get到的数据，没能消费成功，因此需要进行回滚。回滚操作特别简单，只需要将getSequence的位置重置为ackSequence，将getMemSize设置为ackMemSize即可
    public void rollback() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            getSequence.set(ackSequence.get());
            getMemSize.set(ackMemSize.get());
        } finally {
            lock.unlock();
        }
    }

    public void cleanAll() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            putSequence.set(INIT_SEQUENCE);
            getSequence.set(INIT_SEQUENCE);
            ackSequence.set(INIT_SEQUENCE);

            putMemSize.set(0);
            getMemSize.set(0);
            ackMemSize.set(0);
            entries = null;
            // for (int i = 0; i < entries.length; i++) {
            // entries[i] = null;
            // }
        } finally {
            lock.unlock();
        }
    }

    // =================== helper method =================
    // 返回 getSequence 和 ackSequence 二者的较小值
    private long getMinimumGetOrAck() {
        long get = getSequence.get();
        long ack = ackSequence.get();
        return ack <= get ? ack : get;
    }

    /**
     * 查询是否有空位。注意方法参数传入的 sequence 值是：当前 putSequence 值 + 新插入的 event 的记录数
     */
    private boolean checkFreeSlotAt(final long sequence) {
        final long wrapPoint = sequence - bufferSize;  // 1. 检查是否足够的 slot。减去 bufferSize 不能大于 ack 位置，或者换一种说法，减去 bufferSize 不能大于 ack 位置。1.1 首先用 sequence 值减去 bufferSize
        final long minPoint = getMinimumGetOrAck(); // 1.2 获取 get 位置 ack 位置的较小值，事实上，ack 位置总是应该小于等于 get 位置，因此这里总是应该返回的是 ack 位置。
        if (wrapPoint > minPoint) { // 刚好追上一轮。 // 1.3 将 1.1 与 1.2 步得到的值进行比较，如果前者大，说明二者差值已经超过了 bufferSize，不能插入数据，返回 false
            return false;
        } else {
            // 在bufferSize模式上，再增加memSize控制
            if (batchMode.isMemSize()) { // 2. 如果 batchMode 是 MEMSIZE，继续检查是否超出了内存限制。
                final long memsize = putMemSize.get() - ackMemSize.get(); // 2.1 使用 putMemSize 值减去 ackMemSize 值，得到当前保存的 event 事件占用的总内存
                if (memsize < bufferSize * bufferMemUnit) { // 2.2 如果没有超出 bufferSize * bufferMemUnit 内存限制，返回 true，否则返回 false
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;  // 3. 如果 batchMode 不是 MEMSIZE，说明只限制记录数，则直接返回 true
            }
        }
    }

    /**
     * 检查是否存在需要get的数据,并且数量>=batchSize。如果batchMode为ITEMSIZE，则表示只要有有满足batchSize数量的记录数即可，即putSequence - getSequence >= batchSize
     */ // 如果batchMode为MEMSIZE，此时batchSize不再表示记录数，而是bufferMemUnit的个数，也就是说，获取到的event列表占用的总内存要达到batchSize * bufferMemUnit，即putMemSize-getMemSize >= batchSize * bufferMemUnit
    private boolean checkUnGetSlotAt(LogPosition startPosition, int batchSize) {
        if (batchMode.isItemSize()) { // 1. 如果 batchMode 为 ITEMSIZE
            long current = getSequence.get();
            long maxAbleSequence = putSequence.get();
            long next = current;
            if (startPosition == null || !startPosition.getPostion().isIncluded()) { // 第一次订阅之后，需要包含一下start位置，防止丢失第一条记录
                next = next + 1;// 少一条数据。之所以要+1，因为是第一次获取，getSequence的值肯定还是初始值-1，所以要+1变成0之后才是队列的第一个event位置
            }
            // 1.2 理论上只需要满足条件：putSequence - getSequence >= batchSize
            if (current < maxAbleSequence && next + batchSize - 1 <= maxAbleSequence) {  // 1.2.1 先通过current < maxAbleSequence 进行一下简单判断，如果不满足，可以直接返回 false 了
                return true; // 1.2.2 如果1.2.1满足，再通过 putSequence - getSequence >= batchSize 判断是否有足够的数据
            } else {
                return false;
            }
        } else { // 2. 如果 batchMode 为 MEMSIZE
            // 处理内存大小判断
            long currentSize = getMemSize.get();
            long maxAbleSize = putMemSize.get();
            // 2.1 需要满足条件 putMemSize-getMemSize >= batchSize * bufferMemUnit
            if (maxAbleSize - currentSize >= batchSize * bufferMemUnit) {
                return true;
            } else {
                return false;
            }
        }
    }

    private long calculateSize(Event event) {
        // 直接返回binlog中的事件大小
        return event.getRawLength();
    }

    private int getIndex(long sequcnce) {
        return (int) sequcnce & indexMask;
    }
    // 判断 event 是否是 ddl 类型
    private boolean isDdl(EventType type) {
        return type == EventType.ALTER || type == EventType.CREATE || type == EventType.ERASE
               || type == EventType.RENAME || type == EventType.TRUNCATE || type == EventType.CINDEX
               || type == EventType.DINDEX;
    }

    private void profiling(List<Event> events, OP op) {
        long localExecTime = 0L;
        int deltaRows = 0;
        if (events != null && !events.isEmpty()) {
            for (Event e : events) {
                if (localExecTime == 0 && e.getExecuteTime() > 0) {
                    localExecTime = e.getExecuteTime();
                }
                deltaRows += e.getRowsCount();
            }
        }
        switch (op) {
            case PUT:
                putTableRows.addAndGet(deltaRows);
                if (localExecTime > 0) {
                    putExecTime.lazySet(localExecTime);
                }
                break;
            case GET:
                getTableRows.addAndGet(deltaRows);
                if (localExecTime > 0) {
                    getExecTime.lazySet(localExecTime);
                }
                break;
            case ACK:
                ackTableRows.addAndGet(deltaRows);
                if (localExecTime > 0) {
                    ackExecTime.lazySet(localExecTime);
                }
                break;
            default:
                break;
        }
    }

    private enum OP {
        PUT, GET, ACK
    }

    // ================ setter / getter ==================
    public int getBufferSize() {
        return this.bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setBufferMemUnit(int bufferMemUnit) {
        this.bufferMemUnit = bufferMemUnit;
    }

    public void setBatchMode(BatchMode batchMode) {
        this.batchMode = batchMode;
    }

    public void setDdlIsolation(boolean ddlIsolation) {
        this.ddlIsolation = ddlIsolation;
    }

    public boolean isRaw() {
        return raw;
    }

    public void setRaw(boolean raw) {
        this.raw = raw;
    }

    public AtomicLong getPutSequence() {
        return putSequence;
    }

    public AtomicLong getAckSequence() {
        return ackSequence;
    }

    public AtomicLong getPutMemSize() {
        return putMemSize;
    }

    public AtomicLong getAckMemSize() {
        return ackMemSize;
    }

    public BatchMode getBatchMode() {
        return batchMode;
    }

    public AtomicLong getPutExecTime() {
        return putExecTime;
    }

    public AtomicLong getGetExecTime() {
        return getExecTime;
    }

    public AtomicLong getAckExecTime() {
        return ackExecTime;
    }

    public AtomicLong getPutTableRows() {
        return putTableRows;
    }

    public AtomicLong getGetTableRows() {
        return getTableRows;
    }

    public AtomicLong getAckTableRows() {
        return ackTableRows;
    }

}
