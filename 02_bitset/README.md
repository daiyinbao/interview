# 02_bitset

这个目录包含两个基于位图/位集思想的 Java 示例：

- `StreamDedupSystemNio.java`：使用 NIO + 位图对大文件中的整数进行去重，支持断点恢复（位图持久化）。
- `UserSignInSystem.java`：基于内存映射文件（MMAP）的用户签到系统，使用位图记录一年 366 天的签到情况，支持并发读写与定期刷盘。

## 运行环境

- JDK 8+（建议 11+）

## 使用示例

### 1) 大文件整数去重

```bash
# 输入文件为每行一个整数
javac StreamDedupSystemNio.java
java StreamDedupSystemNio input.txt output.txt bitmap.chk
```

### 2) 用户签到系统

```bash
javac UserSignInSystem.java
```

`UserSignInSystem` 是一个可复用的组件类，使用方式见源码中的公开方法：
- `sign(userId, dayOfYear)`
- `isSigned(userId, dayOfYear)`
- `getContinuousSignDays(userId, today)`
- `getMonthSignCount(userId, year, month)`

## 注意事项

- `StreamDedupSystemNio` 只接受 `[0, 1_000_000_000]` 范围内的整数，非法行会被忽略并计数。
- `UserSignInSystem` 使用固定文件格式，初始化时会创建/校验文件头。
