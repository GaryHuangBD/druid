package com.kugou.whaledb.hdfsDirectory;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;

import java.io.EOFException;
import java.io.IOException;

/**
 * Created by Administrator on 2016/4/6 0006.
 */
public abstract class BufferedIndexInput extends IndexInput {
    private int bufferSize = 32768;
    protected byte[] buffer;

    private long bufferStart = 0;       // position in file of buffer
    private int bufferLength = 0;       // end of valid bytes
    private int bufferPosition = 0;     // next byte to read

    public BufferedIndexInput(String resourceDesc, int bufferSize) {
        super(resourceDesc);
        checkBufferSize(bufferSize);
        this.bufferSize = bufferSize;
    }

    private void checkBufferSize(int bufferSize) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("currentBufferSize must be greater than 0 (got " + bufferSize + ")");
        }
    }

    @Override
    public byte readByte() throws IOException {
        if (bufferPosition >= bufferLength)
            refill();
        return buffer[bufferPosition++];
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        readBytes(b, offset, len, true);
    }

    @Override
    public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
        int available = bufferLength - bufferPosition;
        if (len <= available) {
            // the buffer contains enough data to satisfy this request
            if (len > 0) // to allow b to be null if len is 0...
                System.arraycopy(buffer, bufferPosition, b, offset, len);
            bufferPosition += len;
        } else {
            // the buffer does not have enough data. First serve all we've got.
            if (available > 0) {
                System.arraycopy(buffer, bufferPosition, b, offset, available);
                offset += available;
                len -= available;
                bufferPosition += available;
            }
            // and now, read the remaining 'len' bytes:
            if (useBuffer && len < bufferSize) {
                // If the amount left to read is small enough, and
                // we are allowed to use our buffer, do it in the usual
                // buffered way: fill the buffer and copy from it:
                refill();
                if (bufferLength < len) {
                    // Throw an exception when refill() could not read len bytes:
                    System.arraycopy(buffer, 0, b, offset, bufferLength);
                    throw new EOFException("read past EOF: " + this);
                } else {
                    System.arraycopy(buffer, 0, b, offset, len);
                    bufferPosition = len;
                }
            } else {
                // The amount left to read is larger than the buffer
                // or we've been asked to not use our buffer -
                // there's no performance reason not to read it all
                // at once. Note that unlike the previous code of
                // this function, there is no need to do a seek
                // here, because there's no need to reread what we
                // had in the buffer.
                long after = bufferStart + bufferPosition + len;
                if (after > length())
                    throw new EOFException("read past EOF: " + this);
                readInternal(b, offset, len);
                bufferStart = after;
                bufferPosition = 0;
                bufferLength = 0;                    // trigger refill() on read
            }
        }
    }

    @Override
    public int readInt() throws IOException {
        if (4 <= bufferLength - bufferPosition) {
            return (buffer[(bufferPosition++)] & 0xFF) << 24 | (buffer[(bufferPosition++)] & 0xFF) << 16 | (buffer[(bufferPosition++)] & 0xFF) << 8 | buffer[(bufferPosition++)] & 0xFF;
        }
        return super.readInt();
    }

    @Override
    public long readLong() throws IOException {
        if (8 <= (bufferLength - bufferPosition)) {
            final int i1 = ((buffer[bufferPosition++] & 0xff) << 24) | ((buffer[bufferPosition++] & 0xff) << 16) |
                    ((buffer[bufferPosition++] & 0xff) << 8) | (buffer[bufferPosition++] & 0xff);
            final int i2 = ((buffer[bufferPosition++] & 0xff) << 24) | ((buffer[bufferPosition++] & 0xff) << 16) |
                    ((buffer[bufferPosition++] & 0xff) << 8) | (buffer[bufferPosition++] & 0xff);
            return (((long) i1) << 32) | (i2 & 0xFFFFFFFFL);
        } else {
            return super.readLong();
        }
    }

    @Override
    public int readVInt() throws IOException {
        if (5 <= bufferLength - bufferPosition) {
            int i = buffer[(bufferPosition++)];
            int j = i & 0x7F;
            for (int k = 7; (i & 0x80) != 0; k += 7) {
                i = buffer[(bufferPosition++)];
                j |= (i & 0x7F) << k;
            }
            return j;
        }
        return super.readVInt();
    }

    @Override
    public long readVLong() throws IOException {
        if (9 <= bufferLength - bufferPosition) {
            int i = buffer[(bufferPosition++)];
            long l = i & 0x7F;
            for (int j = 7; (i & 0x80) != 0; j += 7) {
                i = buffer[(bufferPosition++)];
                l |= (i & 0x7F) << j;
            }
            return l;
        }
        return super.readVLong();
    }

    private void refill() throws IOException {
        long start = bufferStart + bufferPosition;
        long end = start + bufferSize;
        if (end > length())  // don't read past EOF
            end = length();
        int newLength = (int) (end - start);
        if (newLength <= 0)
            throw new EOFException("read past EOF: " + this);

        if (buffer == null) {
            buffer = new byte[bufferSize];
            seekInternal(bufferStart);
        }
        readInternal(buffer, 0, newLength);
        bufferLength = newLength;
        bufferStart = start;
        bufferPosition = 0;
    }

    @Override
    public void close() throws IOException {
        finalize();
        buffer = null;
    }

    @Override
    public long getFilePointer() {
        return bufferStart + bufferPosition;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos >= bufferStart && pos < (bufferStart + bufferLength))
            bufferPosition = (int) (pos - bufferStart);  // seek within buffer
        else {
            bufferStart = pos;
            bufferPosition = 0;
            bufferLength = 0;  // trigger refill() on read()
            seekInternal(pos);
        }
    }

    @Override
    public IndexInput clone() {
        BufferedIndexInput clone = (BufferedIndexInput) super.clone();
        clone.buffer = null;
        clone.bufferLength = 0;
        clone.bufferPosition = 0;
        clone.bufferStart = getFilePointer();
        return clone;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        return new SlicedIndexInput(sliceDescription, this, offset, length, bufferSize);
    }

    protected abstract void finalize() throws IOException;

    protected abstract void seekInternal(long paramLong) throws IOException;

    protected abstract void readInternal(byte[] b, int offset, int len) throws IOException;

    protected final int flushBuffer(IndexOutput out, long numBytes) throws IOException {
        int toCopy = bufferLength - bufferPosition;
        if (toCopy > numBytes) {
            toCopy = (int) numBytes;
        }
        if (toCopy > 0) {
            out.writeBytes(buffer, bufferPosition, toCopy);
            bufferPosition += toCopy;
        }
        return toCopy;
    }

    private static final class SlicedIndexInput extends BufferedIndexInput implements RandomAccessInput {
        IndexInput base;
        long fileOffset;
        long length;

        SlicedIndexInput(String sliceDescription, IndexInput base, long offset, long length, int bufferSize) throws IOException {
            super((sliceDescription == null) ? base.toString() : (base.toString() + " [slice=" + sliceDescription + "]"), bufferSize);
            if (offset < 0 || length < 0 || offset + length > base.length()) {
                throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: " + base);
            }
            this.base = base.clone();
            base.seek(offset);
            this.fileOffset = offset;
            this.length = length;
        }

        @Override
        public SlicedIndexInput clone() {
            SlicedIndexInput clone = (SlicedIndexInput) super.clone();
            clone.base = base.clone();
            clone.fileOffset = fileOffset;
            clone.length = length;
            return clone;
        }

        @Override
        protected void finalize() throws IOException {
            base.close();
        }

        @Override
        protected void readInternal(byte[] b, int offset, int len) throws IOException {
            long start = getFilePointer();
            base.seek(fileOffset + start);
            base.readBytes(b, offset, len, false);
        }

        @Override
        protected void seekInternal(long pos) throws IOException {
        }

        @Override
        public long length() {
            return length;
        }

        public byte readByte(long pos) throws IOException {
            seek(pos);
            return readByte();
        }

        public short readShort(long pos) throws IOException {
            seek(pos);
            return readShort();
        }

        public int readInt(long pos) throws IOException {
            seek(pos);
            return readInt();
        }

        public long readLong(long pos) throws IOException {
            seek(pos);
            return readLong();
        }
    }
}
