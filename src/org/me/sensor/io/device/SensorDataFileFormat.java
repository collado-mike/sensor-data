package org.me.sensor.io.device;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

/**
 * FileFormat that provides a writer and reader for a file that contains a time series of sensor data. Many sensors may
 * write to the same file. All sensors that will write to the file will need to be registered at the time of the
 * writer's creation. Attempts to write data for sensors that weren't registered will cause an exception to be thrown.
 * <p>
 * The format's main goal is to avoid data loss, so no buffering is implemented. All values are written immediately to
 * the stream and in the rawest form. The output of this writer is non-human readable binary with the following structure
 * <p>
 * HEADER BYTES - 3 bytes including two magic bytes and one byte for the file format version.
 * SENSOR ID MAP- the sensors are written in an ascii comma delimited string with key=value pairs.
 * NEWLINE- this indicates the end of the header and the beginning of the valid data.
 * DATA
 * <p>
 * Data is stored in fixed width columns, with four columns
 * <p>
 * <ol>
 * <li>1-byte prefix, which conforms to the {@link PREFIX} enum. This indicates the type of numeric data
 * (integer or floating point)</li>
 * <li>1-byte sensor id, which references the values in the map in the header</li>
 * <li>8-byte timestamp- the time of the event in epoch milliseconds</li>
 * <li>8-byte value - the actual sensor value</li>
 * </ol>
 * <p>
 * The data uses fixed width, in part because it allows the file to be splittable and processed in chunks across hosts.
 * All processors need to read the header data, but then the actual data itself can be split into chunks and processors
 * using seekable input streams can skip straight to the section of the data that it has been assigned to. Note that
 * gzipping will break this splitability.
 * <p>
 * <b>>Note</b>- one of the alternative designs included allowing for a growing list of sensor data so that new sensors could
 * come online and start firing values and the writer could simply handle it. This could be still be handled cheaply
 * by simply writing the sensor value into the data the first time we see it, then using backreferences to the computed
 * id for all values after that. Unfortunately, the data is no longer in fixed width columns, breaking our ability to
 * skip around to specific points in the file, so that design was discarded. If the files turn out to be small-ish
 * and the ability to skip around to values at random is less than useful, we could consider switching to the
 * alternative design.
 */
public final class SensorDataFileFormat {
    /**
     * We version our file format because we're not insane.
     */
    private static final byte VERSION = (byte) 1;

    /**
     * Every file starts with the magic bytes 0xFE 0xED. Why FEED? cause it's a data feed.
     * Following is the file version
     */
    private static final byte[] HEADER = {(byte) 0xFE, (byte) 0xED, VERSION};

    public enum PREFIX {
        LONG(1),
        DOUBLE(2);

        private byte prefix;

        private PREFIX(int prefix) {
            this.prefix = (byte) prefix;
        }

        public void write(DataOutputStream out) throws IOException {
            out.write(prefix);
        }

        public void writeValue(DataOutputStream out, Number value) throws IOException {
            switch (this) {
                case LONG:
                    out.writeLong(value.longValue());
                case DOUBLE:
                    out.writeDouble(value.doubleValue());
                default:
                    throw new IllegalStateException("There are only two values for this enum.");
            }
        }
        
        Number read(DataInputStream in) throws IOException {
            switch (this) {
                case LONG:
                    return in.readLong();
                case DOUBLE:
                    return in.readDouble();
                default:
                    throw new IllegalStateException("There are only two values for this enum.");
            }
        }

        public static PREFIX forByte(byte value) {
            switch (value) {
                case 1:
                    return LONG;
                case 2:
                    return DOUBLE;
                default:
                    throw new IllegalArgumentException("Unknown PREFIX type specified " + value);
            }
        }
    }

    /**
     * Data model returned by the reader and iterator class. 
     */
    public static final class SensorData {
        private final String sensor;
        private final long timestamp;
        private final Number value;
        private final PREFIX type;

        public SensorData(String sensor, long timestamp, Number value, PREFIX type) {
            this.sensor = sensor;
            this.timestamp = timestamp;
            this.value = value;
            this.type = type;
        }

        public String getSensor() {
            return sensor;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public Number getValue() {
            return value;
        }

        public PREFIX getType() {
            return type;
        }
    }

    private SensorDataFileFormat() {
        // Actual file format is not instantiable. Only the writer and reader classes can be instantiated, and only via
        // static initializers.
    }

    /**
     * Construct a new writer that can handle input from any of the specified sensors. Only the specified sensors will
     * be allowed to write into this writer. New sensors will need to be added to another instance of the writer and
     * will end up in a separate file.
     *
     * @param out            a valid output stream.
     * @param validSensorIds
     * @return
     */
    public static final Writer newWriter(OutputStream out, Set<String> validSensorIds) throws IOException {
        return new Writer(out, validSensorIds);
    }

    /**
     * Construct a new reader that can consume an input stream of data that was written by our writer format.
     */
    public static final Reader newReader(InputStream in) throws IOException {
        return new Reader(in);
    }

    /**
     * Writes a file that conforms to the specification in the {@link SensorDataFileFormat} class docs. This class
     * is immutable, so its thread safety is dictated by its underlying {@link OutputStream}.
     */
    public static final class Writer implements Closeable {
        private final DataOutputStream out;
        private final Set<String> validSensorIds;
        private final Map<String, Byte> sensorIdMap = new HashMap<>();

        public Writer(OutputStream out, Set<String> validSensorIds) throws IOException {
            this.out = new DataOutputStream(out);
            this.validSensorIds = validSensorIds;

            // Write the header info so the reader knows that this is a valid file
            out.write(HEADER);


            // write the sensor id map in the header so the reader knows which sensor the data belongs to
            int id = 0;
            StringBuilder builder = new StringBuilder();
            for (String sId : validSensorIds) {
                sensorIdMap.put(sId, (byte) id);
                builder.append(sId + "=" + id + ",");
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.append('\n');
            this.out.write(builder.toString().getBytes(Charset.forName("US-ASCII")));
        }

        public synchronized void write(String sensor, long timestamp, int value) throws IOException {
            write(sensor, timestamp, (long) value);
        }

        public synchronized void write(String sensor, long timestamp, long value) throws IOException {
            if (!validSensorIds.contains(sensor)) {
                throw new IllegalArgumentException("Invalid sensor id " + sensor +
                        ", this instance can only support " + validSensorIds);
            }
            PREFIX.LONG.write(out);
            out.write(sensorIdMap.get(sensor).byteValue());
            out.writeLong(timestamp);
            out.writeLong(value);
        }

        public synchronized void write(String sensor, long timestamp, float value) throws IOException {
            write(sensor, timestamp, (double) value);
        }

        public synchronized void write(String sensor, long timestamp, double value) throws IOException {
            if (!validSensorIds.contains(sensor)) {
                throw new IllegalArgumentException("Invalid sensor id " + sensor +
                        ", this instance can only support " + validSensorIds);
            }
            PREFIX.DOUBLE.write(out);
            out.write(sensorIdMap.get(sensor).byteValue());
            out.writeLong(timestamp);
            out.writeDouble(value);
        }

        public synchronized void close() throws IOException {
            this.out.close();
        }
    }

    /**
     * Reader for the {@link SensorDataFileFormat}. This reader is immutable, so its thread safety is dicated by the
     * underlying {@link InputStream}.
     */
    public static final class Reader implements Closeable, Iterable<SensorData> {
        private final DataInputStream in;
        private final Map<Byte, String> sensorIdMap = new HashMap<>();
        private final int headerBytes;

        public Reader(InputStream in) throws IOException {
            this.in = new DataInputStream(in);
            byte[] header = new byte[3];
            this.in.read(header, 0, 3);
            if (!Arrays.equals(HEADER, header)) {
                throw new IllegalArgumentException("The supplied input stream does not point to a valid sensor data file. " +
                        "Expected header bytes " + Arrays.toString(HEADER) + " but found " + Arrays.toString(header));
            }
            StringBuilder builder = new StringBuilder();
            char lastRead = (char) this.in.readByte();
            int headerBytesRead = 4;
            Byte id;
            while (lastRead != '\n') {
                if (lastRead == ',') {
                    continue;
                }
                if (lastRead == '=') {
                    sensorIdMap.put(this.in.readByte(), builder.toString());
                    continue;
                }
                builder.append(lastRead);
                lastRead = (char) this.in.readByte();
                headerBytesRead++;
            }
            this.headerBytes = headerBytesRead;
        }

        /**
         * Method to skip to a specified chunk. The length of the entire stream must be known to skip to a specified
         * chunk. Chunk indexes are 0-based, so chunk 0 is the first chunk.
         * @param chunk
         * @param totalChunks
         * @param streamLength
         * @throws IOException
         */
        public void skipTo(int chunk, int totalChunks, long streamLength) throws IOException {
            long dataSize = streamLength - headerBytes;

            long startPos = chunk * (dataSize / totalChunks);
            in.skipBytes((int) startPos);
        }

        public SensorData readNext() throws IOException {
            // try to read the next byte. If there's no byte, we've run out of records.
            byte b = in.readByte();
            if (b <= 0) {
                return null;
            }
            PREFIX prefix = PREFIX.forByte(b);
            String sensor = sensorIdMap.get(in.readByte());
            long timestamp = in.readLong();
            Number value = prefix.read(in);
            return new SensorData(sensor, timestamp, value, prefix);
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public Iterator iterator() {
            return new ReaderIterator(this);
        }
    }

    private static final class ReaderIterator implements Iterator<SensorData> {
        private final Reader reader;
        private boolean hasNext = false;
        private SensorData next;

        private ReaderIterator(Reader reader) {
            this.reader = reader;
        }

        @Override
        public boolean hasNext() {
            if (!hasNext) {
                return false;
            }
            try {
                next = reader.readNext();
            } catch (IOException e) {
                System.out.println("Exception caught reading next value. Marking the iterator as empty");
                e.printStackTrace();
            }
            hasNext = next != null;
            return hasNext;
        }

        @Override
        public SensorData next() {
            return next;
        }
    }

}
