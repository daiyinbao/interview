# 任务调度系统（Task Scheduler）

目标：一步一步实现一个生产级并发任务调度系统，核心思想参考 `ThreadPoolExecutor + BlockingQueue`，并补充重试/超时与单元测试。

项目结构：

```
src/main/java/scheduler/
├── TaskScheduler.java
├── Worker.java
├── Task.java
├── TaskState.java
├── TaskResult.java
├── TaskQueue.java
├── TaskIdGenerator.java
└── Demo.java
src/test/java/scheduler/
├── TaskIdGeneratorTest.java
├── TaskSchedulerSuccessTest.java
└── TaskSchedulerRetryTimeoutTest.java
```

---

**1. 系统架构图（ASCII流程图）**

```
                submit(task)
                     │
                     ▼
              TaskScheduler
                     │
                     ▼
              BlockingQueue
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
     Worker1      Worker2      Worker3
        │            │            │
        ▼            ▼            ▼
   异步执行Task  异步执行Task  异步执行Task
        │
        ▼
ConcurrentHashMap（更新状态）
```

---

**2. 任务执行流程图（ASCII流程图）**

```
submit(task)
    │
    ▼
生成TaskId（AtomicInteger CAS）
    │
    ▼
存入BlockingQueue
    │
    ▼
Worker线程take()
    │
    ▼
Semaphore.acquire()
    │
    ▼
CompletableFuture异步执行Task
    │
    ▼
更新状态ConcurrentHashMap
    │
    ▼
CountDownLatch.countDown()
```

---

**3. ThreadPoolExecutor原理解释**

- 为什么使用Worker线程  
Worker线程是线程池的“常驻线程”，负责从队列中循环取任务执行，避免频繁创建与销毁线程的开销。

- 为什么使用BlockingQueue  
任务提交与执行解耦，生产者（submit）和消费者（worker）通过阻塞队列自然衔接，避免忙等。

- 为什么使用线程池而不是每次创建线程  
线程创建与销毁昂贵，线程池复用线程，性能稳定且可控（并发数、队列长度可配置）。

---

**4. BlockingQueue原理解释**

- `put()`  
队列满时阻塞，直到有空间。

- `take()`  
队列空时阻塞，直到有元素。

- 如何阻塞线程  
内部使用锁与条件变量（`Condition`）挂起线程，满足条件后再唤醒。

- 如何保证线程安全  
入队/出队操作在锁保护下执行，保证同一时刻只有一个线程修改队列结构。

---

**5. CAS机制解释**

- AtomicInteger如何工作  
AtomicInteger底层使用CPU级CAS指令，比较内存中的值是否等于期望值，若相等则更新。

- CAS执行流程图：

```
读取值
  │
  ▼
比较值
  │
  ▼
更新值
```

---

**6. AQS原理解释**

- ReentrantLock  
基于AQS的state表示锁占用，使用CLH队列管理阻塞线程。

- Semaphore  
基于AQS的state表示许可数量，获取许可时CAS减1，释放时加1。

- CountDownLatch  
基于AQS的state表示计数，countDown()减1，await()等待state为0。

底层统一依赖AQS的FIFO等待队列与CAS更新state实现线程安全。

---

**7. 任务调度完整执行流程图**

```
用户submit任务
    │
    ▼
任务进入队列
    │
    ▼
Worker获取任务
    │
    ▼
异步执行任务
    │
    ▼
更新状态
    │
    ▼
通知完成
```

---

**补充说明：CompletableFuture原理**

- 如何实现异步执行  
通过 `CompletableFuture.supplyAsync()` 将任务提交给异步线程池执行。

- 如何避免阻塞  
Worker线程只负责调度与状态更新，真实执行交给异步线程池，完成后回调更新状态。

---

**补充说明：重试与超时机制**

- 重试  
每个任务可配置 `maxRetries`，失败后会按次数重试，最终失败才进入FAILED状态。

- 超时  
每个任务可配置 `timeoutMillis`，单次执行超过超时则取消并视为失败（可触发重试）。

---

**运行Demo**

Demo模拟提交100个任务、5个Worker并发执行，并打印任务状态变化与执行时间。

示例输出：

```
Task 1 RUNNING thread=worker-1
Task 1 SUCCESS thread=worker-1
```

---

**运行单元测试**

使用Maven：

```
mvn test
```
