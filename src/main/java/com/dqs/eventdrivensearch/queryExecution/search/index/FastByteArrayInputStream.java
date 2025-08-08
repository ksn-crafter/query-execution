package com.dqs.eventdrivensearch.queryExecution.search.index;

import java.io.InputStream;

public class FastByteArrayInputStream extends InputStream {
    private final byte[] buffer;
    private int pos;
    private final int limit;

    public FastByteArrayInputStream(byte[] buf) {
        this.buffer = buf;
        this.pos = 0;
        this.limit = buf.length;
    }

    @Override
    public int read() {
        return (pos < limit) ? (buffer[pos++] & 0xFF) : -1;
    }

    @Override
    public int read(byte[] buffer, int offset, int len) {
        if (pos >= limit) {
            return -1;
        }

        int available = limit - pos;
        if (len > available) {
            len = available;
        }
        System.arraycopy(this.buffer, pos, buffer, offset, len);
        pos += len;
        return len;
    }

    @Override
    public int available() {
        return limit - pos;
    }
}
