package com.dqs.eventdrivensearch.queryExecution.search.io;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput) // ops/sec
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class S3SearchResultWriterTest {

    private final List<String> documentIds = new ArrayList<>();

    @Setup(Level.Iteration)
    public void setup() {
        for (int count = 1; count <= 5000; count++) {
            documentIds.add("abcdefghijklmnopqrstuvwxyz");
        }
    }

    @Benchmark
    public void run(Blackhole blackhole) {
        blackhole.consume(S3SearchResultWriter.getString(documentIds, "some path"));
    }

    @Benchmark
    public void runV2(Blackhole blackhole) {
        blackhole.consume(S3SearchResultWriter.getStringV2(documentIds, "some path"));
    }

    @Benchmark
    public void runV3(Blackhole blackhole) throws IOException {
        blackhole.consume(S3SearchResultWriter.getStringV3(documentIds, "some path"));
    }
}