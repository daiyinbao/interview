import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.time.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

public class UserSignInSystem implements Closeable {
    private static final int MAGIC = 0x53494E31; // "SIN1"
    private static final int VERSION = 1;
    // Fixed-size header so user records start at a stable offset.
    private static final int HEADER_BYTES = 4096;

    private static final int DAYS = 366;
    private static final int BYTES_FOR_DAYS = (DAYS + 7) / 8; // 46
    // Per-user fixed length (64-byte aligned) to keep offsets stable.
    private static final int USER_BYTES = 64;

    private final int maxUsers;
    private final Path filePath;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final MappedByteBuffer buffer;

    // Segment read/write locks to reduce contention under high concurrency.
    private final int segmentSizeUsers;
    private final ReentrantReadWriteLock[] locks;

    private final ScheduledExecutorService scheduler;

    public UserSignInSystem(Path filePath, int maxUsers, int segmentSizeUsers, long flushIntervalMs) throws IOException {
        if (maxUsers <= 0) {
            throw new IllegalArgumentException("maxUsers must be > 0");
        }
        if (segmentSizeUsers <= 0) {
            throw new IllegalArgumentException("segmentSizeUsers must be > 0");
        }
        this.maxUsers = maxUsers;
        this.filePath = filePath;
        this.segmentSizeUsers = segmentSizeUsers;
        int lockCount = (maxUsers + segmentSizeUsers - 1) / segmentSizeUsers;
        this.locks = new ReentrantReadWriteLock[lockCount];
        for (int i = 0; i < lockCount; i++) {
            locks[i] = new ReentrantReadWriteLock();
        }

        // Ensure parent dir exists before mapping.
        Files.createDirectories(filePath.getParent());
        this.raf = new RandomAccessFile(filePath.toFile(), "rw");
        long totalSize = HEADER_BYTES + ((long) maxUsers) * USER_BYTES;
        if (raf.length() != totalSize) {
            raf.setLength(totalSize);
        }
        this.channel = raf.getChannel();
        // Memory-map the whole file for zero-copy random access.
        this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, totalSize);

        initHeader();

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "signin-flush");
            t.setDaemon(true);
            return t;
        });
        if (flushIntervalMs > 0) {
            scheduler.scheduleAtFixedRate(this::flushQuietly, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    private void initHeader() throws IOException {
        // Initialize header if first time; otherwise validate for compatibility.
        int currentMagic = buffer.getInt(0);
        int currentVersion = buffer.getInt(4);
        if (currentMagic != MAGIC || currentVersion != VERSION) {
            buffer.putInt(0, MAGIC);
            buffer.putInt(4, VERSION);
            buffer.putInt(8, maxUsers);
            buffer.putInt(12, USER_BYTES);
            buffer.putInt(16, DAYS);
            // Persist header immediately to avoid partial initialization on crash.
            buffer.force();
        } else {
            int storedMaxUsers = buffer.getInt(8);
            if (storedMaxUsers != maxUsers) {
                throw new IOException("File maxUsers mismatch. file=" + storedMaxUsers + " expected=" + maxUsers);
            }
        }
    }

    public void sign(int userId, int dayOfYear) {
        checkUserId(userId);
        checkDay(dayOfYear);
        int userIndex = userId - 1;
        ReentrantReadWriteLock lock = lockForUser(userIndex);
        lock.writeLock().lock();
        try {
            int bitIndex = dayOfYear - 1;
            int byteIndex = bitIndex / 8;
            int bitInByte = bitIndex % 8;
            long base = userBaseOffset(userIndex);
            // Update a single bit in the user's bitmap.
            int pos = (int) (base + byteIndex);
            byte b = buffer.get(pos);
            b = (byte) (b | (1 << bitInByte));
            buffer.put(pos, b);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean isSigned(int userId, int dayOfYear) {
        checkUserId(userId);
        checkDay(dayOfYear);
        int userIndex = userId - 1;
        ReentrantReadWriteLock lock = lockForUser(userIndex);
        lock.readLock().lock();
        try {
            return isSignedNoLock(userIndex, dayOfYear);
        } finally {
            lock.readLock().unlock();
        }
    }

    public int getContinuousSignDays(int userId, LocalDate today) {
        if (today == null) {
            throw new IllegalArgumentException("today must not be null");
        }
        checkUserId(userId);
        int userIndex = userId - 1;
        int dayOfYear = today.getDayOfYear();
        ReentrantReadWriteLock lock = lockForUser(userIndex);
        lock.readLock().lock();
        try {
            int count = 0;
            // Walk backward from today until the first unsigned day.
            for (int d = dayOfYear; d >= 1; d--) {
                if (isSignedNoLock(userIndex, d)) {
                    count++;
                } else {
                    break;
                }
            }
            return count;
        } finally {
            lock.readLock().unlock();
        }
    }

    public int getMonthSignCount(int userId, int month) {
        int year = Year.now().getValue();
        return getMonthSignCount(userId, year, month);
    }

    public int getMonthSignCount(int userId, int year, int month) {
        checkUserId(userId);
        if (month < 1 || month > 12) {
            throw new IllegalArgumentException("month must be 1-12");
        }
        int userIndex = userId - 1;
        YearMonth ym = YearMonth.of(year, month);
        int daysInMonth = ym.lengthOfMonth();

        ReentrantReadWriteLock lock = lockForUser(userIndex);
        lock.readLock().lock();
        try {
            int count = 0;
            // Count signed days within the month.
            for (int day = 1; day <= daysInMonth; day++) {
                LocalDate date = LocalDate.of(year, month, day);
                int dayOfYear = date.getDayOfYear();
                if (isSignedNoLock(userIndex, dayOfYear)) {
                    count++;
                }
            }
            return count;
        } finally {
            lock.readLock().unlock();
        }
    }

    // Force OS to persist memory-mapped changes to disk.
    public void flush() {
        buffer.force();
    }

    private void flushQuietly() {
        try {
            flush();
        } catch (Throwable ignored) {
        }
    }

    private boolean isSignedNoLock(int userIndex, int dayOfYear) {
        int bitIndex = dayOfYear - 1;
        int byteIndex = bitIndex / 8;
        int bitInByte = bitIndex % 8;
        long base = userBaseOffset(userIndex);
        int pos = (int) (base + byteIndex);
        byte b = buffer.get(pos);
        return ((b >>> bitInByte) & 1) == 1;
    }

    private long userBaseOffset(int userIndex) {
        // Each user occupies a fixed 64-byte slot.
        return HEADER_BYTES + ((long) userIndex) * USER_BYTES;
    }

    private ReentrantReadWriteLock lockForUser(int userIndex) {
        int seg = userIndex / segmentSizeUsers;
        return locks[seg];
    }

    private void checkUserId(int userId) {
        if (userId < 1 || userId > maxUsers) {
            throw new IllegalArgumentException("userId out of range: " + userId);
        }
    }

    private void checkDay(int dayOfYear) {
        if (dayOfYear < 1 || dayOfYear > DAYS) {
            throw new IllegalArgumentException("dayOfYear must be 1-" + DAYS);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            scheduler.shutdownNow();
        } catch (Throwable ignored) {
        }
        try {
            flush();
        } catch (Throwable ignored) {
        }
        channel.close();
        raf.close();
    }

    // Simple demo
    public static void main(String[] args) throws Exception {
        Path path = Paths.get("E:/java_Interview/02_bitset/signin.dat");
        int maxUsers = 10_000_000;
        int segmentSizeUsers = 8192;
        long flushIntervalMs = 5000;

        try (UserSignInSystem system = new UserSignInSystem(path, maxUsers, segmentSizeUsers, flushIntervalMs)) {
            int userId = 1;
            LocalDate today = LocalDate.now();
            int dayOfYear = today.getDayOfYear();

            system.sign(userId, dayOfYear);
            if (dayOfYear > 1) {
                system.sign(userId, dayOfYear - 1);
            }
            if (dayOfYear > 2) {
                system.sign(userId, dayOfYear - 2);
            }

            System.out.println("isSigned today: " + system.isSigned(userId, dayOfYear));
            System.out.println("continuous: " + system.getContinuousSignDays(userId, today));
            System.out.println("month count: " + system.getMonthSignCount(userId, today.getMonthValue()));

            system.flush();
        }
    }
}
