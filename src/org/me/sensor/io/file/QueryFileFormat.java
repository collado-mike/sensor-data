package org.me.sensor.io.file;

import org.me.sensor.io.device.SensorDataFileFormat;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * FileFormat that is built for querying. This file format actually generates two files for a given batch of sensor data.
 *
 * The first file is ordered by time and includes all values. This file format includes a magic header (so we know which
 * file format and version we're reading), the sensor id, a base timestamp, and a publish frequency in counts per second
 * (e.g., one value per millisecond will cause us to write a frequency value of 1000). For our purposes, we assume all
 * values are published at the indicated frequency and that no time values are skipped and none are repeated or offset.
 * Based on this assumption, we omit timestamps from the data and simply write the values. Since we expect that many
 * values are repeated in a high frequency reporter, we use a run-length encoding to further reduce the size of this
 * file. Our run-length encoding uses two bytes for the run length counter, so will support up to 2^16 - 1 repetitions.
 * Low frequency reporters are less likely to see repeated values, but will also not generate as much data, so this is
 * a reasonable tradeoff. This file format is generally useful for iterating over the time values, e.g., to generate a
 * plot.
 *
 * The second file is sorted by the values. A base for the timestamp is included in the header, so values are recorded
 * with the offset from the base timestamp. We use four bytes for the timestamp offset, so the maximum time duration is
 * {@link Integer#MAX_VALUE} milliseconds after the base. Because the values are fixed width and sorted by value, this
 * file can be very quickly used to find the top(k) (by simply skipping to the last k values) or to find percentile
 * values (by skipping directly to the percentile).
 *
 * Generating these files may happen in multiple chunks. Since we have to sort the values, all of the values handled by
 * a single chunk must fit in memory.
 *
 * If a given file is chunked among many processors, the files sorted by time can simply be appended one after the other
 * (excluding the header values). Since we don't write the timestamp with the values, we simply need to order the files
 * by the base timestamp and append the data in the correct order.
 *
 * The file sorted by values can be merge-sorted, but the timestamp offsets will need to be re-written to be an offset
 * of the base timestamp that ends up in the final file (it doesn't really matter which timestamp it is, but the
 * earliest base timestamp is a reasonable choice).
 *
 * The write is {@link Closeable}, so data will not be flushed until the {@link #close()} method is called. In
 * particular, the value sorted file will not contain any data until the data is flushed during the {@link #close()}
 * process.
 *
 * This class is definitively NOT thread safe. This class should be isolated to within a single thread, as its state
 * may be non-deterministic if accessed by multiple threads.
 *
 *
 */
public class QueryFileFormat<T extends Number & Comparable> implements Closeable {
    /**
     * We version our files because, well...
     */
    private static final byte VERSION = 1;

    /**
     * Header bytes for the two files we'll generate.
     */
    private static final byte[] TIMESORTED_MAGIC = {(byte)0xFE, (byte)0xED, (byte)0x0A, VERSION};
    private static final byte[] VALUESORTED_MAGIC = {(byte)0xFE, (byte)0xED, (byte)0x0B, VERSION};
    
    private final File outputDirectory;
    private final DataOutputStream timeSortedOut;
    private final DataOutputStream valueSortedOut;
    private final SortedSet<SensorDataFileFormat.SensorData> values;

    private long baseTimestamp;
    private int timestampRate;
    private SensorDataFileFormat.SensorData lastSeen;
    private short runLengthCounter = 0;

    /**
     * @param outputDirectory The directory where all files will be generated.
     * @param sensorId The sensor for which this data is written. All data should be for the same sensor.
     * @param fileSuffix The file suffix, including any numeric suffix, in case this writer is one of many chunks.
     *                   Including a suffix allows each chunk to write a separate file that can be merge-sorted later on.
     * @throws IOException
     */
    public QueryFileFormat(File outputDirectory, String sensorId, String fileSuffix) throws IOException {
        if (!outputDirectory.exists()) {
            throw new IllegalArgumentException("Unable to write data to " + outputDirectory
                    + " because it does not exist");
        }
        this.outputDirectory = outputDirectory;
        timeSortedOut = new DataOutputStream(new FileOutputStream(new File(outputDirectory, "timesorted" + fileSuffix)));
        valueSortedOut = new DataOutputStream(new FileOutputStream(new File(outputDirectory, "valuesorted" + fileSuffix)));

        timeSortedOut.write(TIMESORTED_MAGIC);
        timeSortedOut.write(sensorId.getBytes(Charset.forName("US-ASCII")));

        valueSortedOut.write(VALUESORTED_MAGIC);
        valueSortedOut.write(sensorId.getBytes(Charset.forName("US-ASCII")));
        values = new TreeSet<>((a,b) -> Comparator.<T>naturalOrder().compare((T)a.getValue(), (T)b.getValue()));
    }

    /**
     * Add a value to the files.
     * @param data
     * @throws IOException
     */
    public void add(SensorDataFileFormat.SensorData data) throws IOException {
        if (baseTimestamp == 0) {
            baseTimestamp = data.getTimestamp();
            timeSortedOut.writeLong(baseTimestamp);
            valueSortedOut.writeLong(baseTimestamp);
        } else if (timestampRate == 0) {
            timestampRate = (int)(1000/(data.getTimestamp() - baseTimestamp));
            timeSortedOut.writeInt(timestampRate);
        }
        values.add(data);
        if (lastSeen != null && !lastSeen.equals(data.getValue())) {
            flushRle();
            lastSeen = data;
        } else {
            lastSeen = data;
            runLengthCounter++;
        }
    }

    private void flushRle() throws IOException {
        timeSortedOut.writeShort(runLengthCounter);
        lastSeen.getType().writeValue(timeSortedOut, lastSeen.getValue());
        runLengthCounter = 0;
    }

    private void flushSorted() throws IOException {
        for (SensorDataFileFormat.SensorData value : values) {
            valueSortedOut.writeInt((int)(value.getTimestamp() - baseTimestamp));
            value.getType().writeValue(valueSortedOut, value.getValue());
        }
    }

    public void close() throws IOException {
        try {
            flushRle();
            flushSorted();
        } finally {
            try {
                timeSortedOut.close();
            } finally {
                valueSortedOut.close();
            }
        }
    }
}
