package org.me.sensor.io;

import org.me.sensor.io.device.SensorDataFileFormat;
import org.me.sensor.io.file.QueryFileFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Simple processor, which uses the {@link SensorDataFileFormat} class to read data and writes it all out to
 * subdirectories within a given parent directory. Each sensor found within the file will be routed to a separate
 * {@link QueryFileFormat}, which can be used for querying the data.
 *
 * The data points are written out to blocking queues which are consumed by separate threads. While there's no real
 * benefit, processing-wise, to use multiple threads, there might be some benefit if we were writing out to a remote
 * filesystem, such as S3 or HDFS.
 */
public class FileProcessor {
    private final File dataFile;
    private final File outputDirectory;
    private final ExecutorService executor;

    private volatile boolean finishedProcessing = false;
    private volatile boolean errorInProcessing = false;


    public FileProcessor(File dataFile, File outputDirectory) {
        this.dataFile = dataFile;
        this.outputDirectory = outputDirectory;
        if (!dataFile.exists()) {
            throw new IllegalArgumentException("Invalid data file specified- " + dataFile);
        }
        executor = Executors.newCachedThreadPool(new ThreadFactory() {
            int idx = 0;
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("FileProcessor-" + ++idx);

                // always set the thread to daemon. Otherwise, a queue consumer might sit waiting after the main thread
                // has already crashed.
                t.setDaemon(true);
                return t;
            }
        });
    }

    /**
     * Process the specified chunk of the data.
     * @param chunk
     * @throws IOException
     */
    public void process(int chunk, int totalChunks) throws IOException {
        SensorDataFileFormat.Reader reader = SensorDataFileFormat.newReader(new FileInputStream(dataFile));
        reader.skipTo(chunk, totalChunks, dataFile.length());
        
        Map<String, BlockingQueue<SensorDataFileFormat.SensorData>> outputFileMap = new HashMap<>();
        for (SensorDataFileFormat.SensorData data : reader) {
            if (errorInProcessing) {
                throw new RuntimeException("Unable to process the data file " + dataFile);
            }
            if (!outputFileMap.containsKey(data.getSensor())) {
                BlockingQueue<SensorDataFileFormat.SensorData> queue = new ArrayBlockingQueue<>(1);
                outputFileMap.put(data.getSensor(), queue);
                executor.submit(() -> {
                    QueryFileFormat fileFormat = null;
                    try {
                        fileFormat = new QueryFileFormat(new File(outputDirectory, data.getSensor()),
                                                        data.getSensor(),
                                                        String.valueOf(chunk));
                        while (!finishedProcessing) {
                            SensorDataFileFormat.SensorData value = queue.poll(100, TimeUnit.MILLISECONDS);
                            fileFormat.add(value);
                        }
                    } catch (IOException e) {
                        System.err.println("Unable to complete processing file " + dataFile);
                        e.printStackTrace();
                        errorInProcessing = true;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        if (fileFormat != null) {
                            try {
                                fileFormat.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                });
            }
            try {
                outputFileMap.get(data.getSensor()).offer(data, 100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
                return;
            }
        }
    }
}
