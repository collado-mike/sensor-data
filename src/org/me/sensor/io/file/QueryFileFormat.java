package org.me.sensor.io.file;

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
 *
 */
public class QueryFileFormat {

}
