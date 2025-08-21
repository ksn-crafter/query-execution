package com.dqs.eventdrivensearch.queryExecution.search.index.Directory;


import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.*;

public final class MemorySegmentDirectory extends BaseDirectory {

    // Default chunk size for file growth (power of two is nice).
    private final int chunkSize;

    private final ConcurrentHashMap<String, FileEntry> files = new ConcurrentHashMap<>();

    private final Supplier<MSIndexOutput> outputSupplier;

    private final FunctionWithCounter tempFileName = new FunctionWithCounter();

    public MemorySegmentDirectory() {
        this(new SingleInstanceLockFactory(), 1 << 19); // 512 KiB chunks
    }

    public MemorySegmentDirectory(LockFactory lockFactory, int chunkSize) {
        super(lockFactory);
        if (chunkSize <= 0) throw new IllegalArgumentException("chunkSize must be > 0");
        this.chunkSize = chunkSize;
        this.outputSupplier = () -> new MSIndexOutput(chunkSize);
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        return files.keySet().stream().sorted().toArray(String[]::new);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ensureOpen();
        FileEntry removed = files.remove(name);
        if (removed == null) throw new NoSuchFileException(name);
        removed.free(); // explicit native free
    }

    @Override
    public long fileLength(String name) throws IOException {
        ensureOpen();
        FileEntry e = files.get(name);
        if (e == null) throw new NoSuchFileException(name);
        return e.length();
    }

    public boolean fileExists(String name) {
        ensureOpen();
        return files.containsKey(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        FileEntry e = new FileEntry(name);
        if (files.putIfAbsent(name, e) != null) {
            throw new FileAlreadyExistsException("File already exists: " + name);
        }
        return e.createOutput(outputSupplier);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
            throws IOException {
        ensureOpen();
        while (true) {
            String name = IndexFileNames.segmentFileName(prefix, tempFileName.apply(suffix), "tmp");
            FileEntry e = new FileEntry(name);
            if (files.putIfAbsent(name, e) == null) {
                return e.createOutput(outputSupplier);
            }
        }
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        ensureOpen();
        FileEntry file = files.get(source);
        if (file == null) throw new NoSuchFileException(source);
        if (files.putIfAbsent(dest, file) != null) throw new FileAlreadyExistsException(dest);
        if (!files.remove(source, file))
            throw new IllegalStateException("File replaced concurrently: " + source);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        ensureOpen();
        // No-op (in-memory).
    }

    @Override
    public void syncMetaData() throws IOException {
        ensureOpen();
        // No-op (in-memory).
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        FileEntry e = files.get(name);
        if (e == null) throw new NoSuchFileException(name);
        return e.openInput();
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
        // Free everything deterministically.
        for (FileEntry e : files.values()) {
            e.free();
        }
        files.clear();
    }

    @Override
    public Set<String> getPendingDeletions() {
        return Collections.emptySet();
    }

    // ---------- internals ----------

    private static final class FunctionWithCounter {
        private final AtomicLong counter = new AtomicLong();

        String apply(String suffix) {
            return suffix + "_" + Long.toString(counter.getAndIncrement(), 36);
        }
    }

    private final class FileEntry {
        private final String fileName;

        // One arena per file: close() frees all segments of this file.
        private final Arena arena = Arena.ofConfined();
        private final List<MemorySegment> segments = new ArrayList<>();

        private volatile IndexInput content;
        private volatile long cachedLength;

        FileEntry(String name) {
            this.fileName = name;
        }

        long length() {
            return cachedLength;
        }

        IndexInput openInput() throws IOException {
            IndexInput local = content;
            if (local == null) {
                throw new AccessDeniedException("Can't open a file still open for writing: " + fileName);
            }
            return local.clone();
        }

        IndexOutput createOutput(Supplier<MSIndexOutput> supplier) throws IOException {
            if (this.content != null) {
                throw new IOException("Can only write to a file once: " + fileName);
            }
            final String resName =
                    String.format(
                            Locale.ROOT,
                            "%s output (file=%s)",
                            MemorySegmentDirectory.class.getSimpleName(),
                            fileName);

            MSIndexOutput out = supplier.get();
            out.init(
                    resName,
                    fileName,
                    arena,
                    segments,
                    (totalBytes, buffersForRead) -> {
                        // Build a ByteBuffersIndexInput from our MemorySegments’ ByteBuffer views.
                        ByteBuffersDataInput dataInput = new ByteBuffersDataInput(buffersForRead);
                        String inputName =
                                String.format(
                                        Locale.ROOT,
                                        "%s (file=%s, buffers=%s)",
                                        ByteBuffersIndexInput.class.getSimpleName(),
                                        fileName,
                                        dataInput.toString());
                        this.content = new ByteBuffersIndexInput(dataInput, inputName);
                        this.cachedLength = totalBytes;
                    });
            return out;
        }

        void free() {
            // closes/free all native memory for this file
            arena.close();
        }
    }

    /**
     * An IndexOutput that writes into off-heap MemorySegments in fixed-size chunks. On close, it
     * exposes read buffers as ByteBuffer views (no copy) to build an IndexInput.
     */
    private static final class MSIndexOutput extends IndexOutput {
        private String resourceDesc;
        private String name;

        private Arena arena; // provided by FileEntry
        private List<MemorySegment> segments; // provided by FileEntry
        private final int chunkSize;

        private int writePosInChunk = 0;
        private long filePointer = 0L;
        private MemorySegment currentSeg;

        private final CRC32 crc = new CRC32();

        // callback to finalize: totalBytes + list of ByteBuffers for reading
        interface Finalizer {
            void onClose(long totalBytes, List<ByteBuffer> buffersForRead);
        }

        private Finalizer finalizer;

        MSIndexOutput(int chunkSize) {
            super("unset", "unset");
            this.chunkSize = chunkSize;
        }

        void init(
                String resourceDesc,
                String name,
                Arena arena,
                List<MemorySegment> targetSegments,
                Finalizer fin) {
            this.resourceDesc = resourceDesc;
            this.name = name;
            this.arena = arena;
            this.segments = targetSegments;
            this.finalizer = fin;
            setFieldNamesForIndexOutput(resourceDesc, name);
            allocNewChunk();
        }

        private void setFieldNamesForIndexOutput(String resourceDesc, String name) {
            try {
                var f1 = IndexOutput.class.getDeclaredField("resourceDescription");
                var f2 = IndexOutput.class.getDeclaredField("name");
                f1.setAccessible(true);
                f2.setAccessible(true);
                f1.set(this, resourceDesc);
                f2.set(this, name);
            } catch (ReflectiveOperationException ignore) {
                // If reflection fails (different Lucene version), it only affects toString(); harmless.
            }
        }

        private void allocNewChunk() {
            currentSeg = arena.allocate(chunkSize);
            segments.add(currentSeg);
            writePosInChunk = 0;
        }

        @Override
        public void writeByte(byte b) throws IOException {
            ensureCapacity(1);
            currentSeg.set(ValueLayout.JAVA_BYTE, writePosInChunk, b);
            writePosInChunk += 1;
            filePointer += 1;
            crc.update(b & 0xFF);
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            Objects.checkFromIndexSize(offset, length, b.length);
            int remaining = length;
            int off = offset;
            while (remaining > 0) {
                int space = chunkSize - writePosInChunk;
                if (space == 0) {
                    allocNewChunk();
                    space = chunkSize;
                }
                int toWrite = Math.min(space, remaining);
                MemorySegment src = MemorySegment.ofArray(b).asSlice(off, toWrite);
                currentSeg.asSlice(writePosInChunk, toWrite).copyFrom(src);
                writePosInChunk += toWrite;
                filePointer += toWrite;
                off += toWrite;
                remaining -= toWrite;
            }
            crc.update(b, offset, length);
        }

        private void ensureCapacity(int need) {
            if (writePosInChunk + need > chunkSize) {
                allocNewChunk();
            }
        }

        @Override
        public long getFilePointer() {
            return filePointer;
        }

        @Override
        public long getChecksum() throws IOException {
            return crc.getValue();
        }


        @Override
        public void close() throws IOException {
            List<ByteBuffer> buffers = new ArrayList<>(segments.size());
            long remaining = filePointer;

            for (MemorySegment seg : segments) {
                long sliceLen = Math.min(remaining, seg.byteSize());
                if (sliceLen <= 0) break;

                ByteBuffer view = seg.asByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
                if (sliceLen < seg.byteSize()) {
                    // create an independent view with the truncated limit
                    ByteBuffer dup = view.duplicate().order(ByteOrder.LITTLE_ENDIAN);
                    dup.limit((int) sliceLen);
                    dup.position(0);
                    buffers.add(dup);
                } else {
                    buffers.add(view);
                }
                remaining -= sliceLen;
            }
            if (buffers.isEmpty()) {
                buffers.add(ByteBuffer.allocate(0));
            }

            finalizer.onClose(filePointer, buffers);
        }
    }

    // Tiny helper for ValueLayout (Java 22)
    private static final class ValueLayout {
        static final java.lang.foreign.ValueLayout.OfByte JAVA_BYTE =
                java.lang.foreign.ValueLayout.JAVA_BYTE;
    }
}

