package com.dqs.eventdrivensearch.queryExecution.search.index;

import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Component
public class ScratchBufferPool {

    private final BlockingQueue<ScratchBuffer> pool;

    private final int cfeSize;
    private final int cfsSize;
    private final int siSize;
    private final int segmentsSize;

    public ScratchBufferPool(@Value("${scratch_buffer_pool_size}") int poolSize) {
        this.cfeSize = 2 * 1024;
        this.cfsSize = 7 * 1024 * 1024;
        this.siSize = 2 * 1024;
        this.segmentsSize = 2 * 1024;
        this.pool = new LinkedBlockingQueue<>(poolSize);

        for (int index = 0; index < poolSize; index++) {
            if (!pool.offer(new ScratchBuffer(cfeSize, cfsSize, siSize, segmentsSize))) {
                throw new IllegalStateException("Failed to add an instance of ScratchBuffers to the pool");
            }
        }
    }

    public ScratchBuffer acquire() {
        ScratchBuffer scratch = pool.poll();
        return Objects.requireNonNullElseGet(scratch, () -> new ScratchBuffer(cfeSize, cfsSize, siSize, segmentsSize));
    }

    public void release(ScratchBuffer scratch) {
        if (!pool.offer(scratch)) {
            System.out.println("Scratch buffer dropped; pool is full.");
        }
    }

    @PreDestroy
    public void cleanup() {
        pool.clear();
    }
}