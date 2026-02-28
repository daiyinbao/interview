import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;

// NIO + ByteBuffer 流式去重实现，避免一次性加载文件
public class StreamDedupSystemNio {
    // 允许的整数范围 [0, 1_000_000_000]
    private static final long MIN_VALUE = 0L;
    private static final long MAX_VALUE = 1_000_000_000L;
    // 位图长度：每个值占 1 bit
    private static final int BITMAP_BITS = (int) (MAX_VALUE + 1); // inclusive

    // checkpoint 间隔，控制持久化频率与性能折中
    private static final long CHECKPOINT_EVERY = 5_000_000L;

    // 输入/输出缓冲区大小，影响吞吐
    private static final int IN_BUF_SIZE = 4 * 1024 * 1024;   // 4MB
    private static final int OUT_BUF_SIZE = 4 * 1024 * 1024;  // 4MB

    public static void main(String[] args) throws Exception {
        // 参数：输入文件、输出文件、可选位图文件（用于断点恢复）
        if (args.length < 2) {
            System.out.println("Usage: java StreamDedupSystemNio <inputFile> <outputFile> [bitmapFile]");
            return;
        }

        Path input = Paths.get(args[0]);
        Path output = Paths.get(args[1]);
        Path bitmapFile = args.length >= 3 ? Paths.get(args[2]) : Paths.get("bitmap.chk");

        Bitmap bitmap = new Bitmap(BITMAP_BITS);
        // 若位图文件存在则加载，支持异常退出后恢复
        if (Files.exists(bitmapFile)) {
            bitmap.load(bitmapFile);
        }

        long total = 0;
        long unique = 0;
        long invalid = 0;

        try (FileChannel in = FileChannel.open(input, StandardOpenOption.READ);
             FileChannel out = FileChannel.open(output, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            ByteBuffer inBuf = ByteBuffer.allocateDirect(IN_BUF_SIZE);
            ByteBuffer outBuf = ByteBuffer.allocateDirect(OUT_BUF_SIZE);

            long current = 0;
            boolean hasDigit = false;
            boolean invalidLine = false;

            // NIO 方式读取，逐字节解析数字，避免生成字符串
            while (true) {
                int n = in.read(inBuf);
                if (n == -1) {
                    break;
                }
                inBuf.flip();

                while (inBuf.hasRemaining()) {
                    byte b = inBuf.get();
                    if (b == '\n') {
                        // 遇到 '\\n' 视为一行结束
                        total++;
                        if (hasDigit && !invalidLine) {
                            if (current >= MIN_VALUE && current <= MAX_VALUE) {
                                int v = (int) current;
                                if (!bitmap.get(v)) {
                                    bitmap.set(v);
                                    // 位图判重：首次出现写入输出文件
                                    writeLong(outBuf, out, current);
                                    unique++;
                                }
                            } else {
                                invalid++;
                            }
                        } else {
                            invalid++;
                        }
                        current = 0;
                        hasDigit = false;
                        invalidLine = false;

                        if (total % CHECKPOINT_EVERY == 0) {
                            flushOut(outBuf, out);
                            bitmap.save(bitmapFile);
                        }
                        continue;
                    }

                    // 忽略 '\\r'，兼容 Windows 换行
                    if (b == '\r') {
                        continue;
                    }

                    if (b >= '0' && b <= '9') {
                        hasDigit = true;
                        current = current * 10 + (b - '0');
                        if (current > MAX_VALUE) {
                            invalidLine = true;
                        }
                    } else {
                        // 只接受数字字符，其他均判为非法行
                        invalidLine = true;
                    }
                }

                inBuf.clear();
            }

            // 处理文件末尾无换行的最后一行
            if (hasDigit || invalidLine) {
                total++;
                if (hasDigit && !invalidLine) {
                    if (current >= MIN_VALUE && current <= MAX_VALUE) {
                        int v = (int) current;
                        if (!bitmap.get(v)) {
                            bitmap.set(v);
                            // 位图判重：首次出现写入输出文件
                            writeLong(outBuf, out, current);
                            unique++;
                        }
                    } else {
                        invalid++;
                    }
                } else {
                    invalid++;
                }
            }

            flushOut(outBuf, out);
        }

        bitmap.save(bitmapFile);

        System.out.println("Total lines: " + total);
        System.out.println("Unique: " + unique);
        System.out.println("Invalid: " + invalid);
        System.out.println("Bitmap memory bytes: " + bitmap.memoryBytes());
    }

    // 直接将 long 转为 ASCII 写入 ByteBuffer，避免 String
    private static void writeLong(ByteBuffer outBuf, FileChannel out, long value) throws IOException {
        // max 10 digits for 1_000_000_000
        int len = decimalLength(value);
        if (outBuf.remaining() < len + 1) {
            flushOut(outBuf, out);
        }
        putLongAscii(outBuf, value, len);
        outBuf.put((byte) '\n');
    }

    private static void flushOut(ByteBuffer outBuf, FileChannel out) throws IOException {
        outBuf.flip();
        while (outBuf.hasRemaining()) {
            out.write(outBuf);
        }
        outBuf.clear();
    }

    private static int decimalLength(long v) {
        if (v < 10) return 1;
        if (v < 100) return 2;
        if (v < 1_000) return 3;
        if (v < 10_000) return 4;
        if (v < 100_000) return 5;
        if (v < 1_000_000) return 6;
        if (v < 10_000_000) return 7;
        if (v < 100_000_000) return 8;
        if (v < 1_000_000_000) return 9;
        return 10;
    }

    private static void putLongAscii(ByteBuffer buf, long v, int len) {
        int pos = buf.position() + len - 1;
        for (int i = 0; i < len; i++) {
            int digit = (int) (v % 10);
            buf.put(pos - i, (byte) ('0' + digit));
            v /= 10;
        }
        buf.position(buf.position() + len);
    }

    // 位图采用 long[] 存储，每个 bit 表示一个值是否出现
    public static final class Bitmap {
        private final long[] words;
        private final int bitSize;

        public Bitmap(int bitSize) {
            if (bitSize <= 0) {
                throw new IllegalArgumentException("bitSize must be > 0");
            }
            this.bitSize = bitSize;
            int wordCount = (bitSize + 63) >>> 6;
            this.words = new long[wordCount];
        }

        public boolean get(int index) {
            checkIndex(index);
            int wordIndex = index >>> 6;
            int bit = index & 63;
            long mask = 1L << bit;
            return (words[wordIndex] & mask) != 0L;
        }

        public void set(int index) {
            checkIndex(index);
            int wordIndex = index >>> 6;
            int bit = index & 63;
            long mask = 1L << bit;
            words[wordIndex] |= mask;
        }

        public long memoryBytes() {
            return (long) words.length * Long.BYTES;
        }

        // save/load 用于断点恢复
        public void save(Path file) throws IOException {
            Files.createDirectories(file.getParent() == null ? Paths.get(".") : file.getParent());
            try (FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                ByteBuffer buf = ByteBuffer.allocateDirect(8 * 1024 * 1024);
                int i = 0;
                while (i < words.length) {
                    buf.clear();
                    while (i < words.length && buf.remaining() >= Long.BYTES) {
                        buf.putLong(words[i++]);
                    }
                    buf.flip();
                    while (buf.hasRemaining()) {
                        ch.write(buf);
                    }
                }
            }
        }

        public void load(Path file) throws IOException {
            long expectedBytes = memoryBytes();
            long actualBytes = Files.size(file);
            if (actualBytes != expectedBytes) {
                throw new IOException("Bitmap file size mismatch. expected=" + expectedBytes + " actual=" + actualBytes);
            }
            try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
                ByteBuffer buf = ByteBuffer.allocateDirect(8 * 1024 * 1024);
                int i = 0;
                while (i < words.length) {
                    buf.clear();
                    int read = ch.read(buf);
                    if (read == -1) {
                        throw new EOFException("Unexpected EOF while reading bitmap");
                    }
                    buf.flip();
                    while (buf.remaining() >= Long.BYTES && i < words.length) {
                        words[i++] = buf.getLong();
                    }
                }
            }
        }

        private void checkIndex(int index) {
            if (index < 0 || index >= bitSize) {
                throw new IllegalArgumentException("index out of range: " + index);
            }
        }
    }
}
