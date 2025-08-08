package com.dqs.eventdrivensearch.queryExecution.search.index;

public class ScratchBuffer {
    public byte[] cfe;
    public byte[] cfs;
    public byte[] si;
    public byte[] segments;

    public ScratchBuffer(int cfeSize, int cfsSize, int siSize, int segmentsSize) {
        this.cfe = new byte[cfeSize];
        this.cfs = new byte[cfsSize];
        this.si = new byte[siSize];
        this.segments = new byte[segmentsSize];
    }

    public byte[] ensureCapacity(byte[] current, int required) {
        return (current.length >= required) ? current : new byte[required];
    }
}