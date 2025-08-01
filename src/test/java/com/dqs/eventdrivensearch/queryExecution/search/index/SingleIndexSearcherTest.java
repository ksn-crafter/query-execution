package com.dqs.eventdrivensearch.queryExecution.search.index;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class SingleIndexSearcherTest {

    private byte[] splitFileBytes;

    private SingleIndexSearcher singleIndexSearcher;

    private SingleIndexSearcher singleIndexSearcherWithPool;

    private Path outputPath;

    private Path outputPathWithPool;

    @Setup(Level.Invocation)
    public void setup() throws URISyntaxException, IOException {
        Path splitPath = Paths.get(getClass().getClassLoader().getResource("split_0").toURI());
        splitFileBytes = Files.readAllBytes(splitPath);

        singleIndexSearcher = new SingleIndexSearcher(null, null, null, null);

        ScratchBufferPool scratchBufferPool = new ScratchBufferPool(8);
        singleIndexSearcherWithPool = new SingleIndexSearcher(null, null, null, scratchBufferPool);

        outputPath = Files.createTempDirectory("benchmark-");
        outputPathWithPool = Files.createTempDirectory("benchmark-with-pool-");
    }

    @TearDown(Level.Invocation)
    public void tearDown() {
        deleteDirectory(outputPath.toFile());
        deleteDirectory(outputPathWithPool.toFile());
    }

    @Benchmark
    public void run(Blackhole blackhole) throws IOException {
        singleIndexSearcher.readSplitAndWriteLuceneSegment(outputPath, splitFileBytes, "split_0");
    }

    @Benchmark
    public void runV2(Blackhole blackhole) throws IOException {
        singleIndexSearcherWithPool.readSplitAndWriteLuceneSegmentV2(outputPathWithPool, splitFileBytes, "split_0");
    }

    private void deleteDirectory(File directory) {
        if (directory == null || !directory.exists()) return;

        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    this.deleteDirectory(file);
                } else {
                    if (!file.delete()) {
                        System.err.println("Failed to delete: " + file.getAbsolutePath());
                    }
                }
            }
        }
        if (!directory.delete()) {
            System.err.println("Failed to delete directory: " + directory.getAbsolutePath());
        }
    }
}