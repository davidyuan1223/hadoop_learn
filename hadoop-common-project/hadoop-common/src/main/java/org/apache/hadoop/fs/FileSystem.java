package org.apache.hadoop.fs;

import com.apache.hadoop.classification.InterfaceAudience;
import com.apache.hadoop.classification.InterfaceStability;
import com.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.impl.FutureDataInputStreamBuilderImpl;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.DelegationTokenIssuer;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.org.checkerframework.framework.qual.PreconditionAnnotation;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.kerby.config.Conf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings("DeprecatedIsStillUsed")
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileSystem extends Configured implements Closeable,DelegationTokenIssuer,PathCapabilities{





    public static final class Statistics{
        private final String scheme;
        private final StatisticsData rootData;
        @SuppressWarnings("ThreadLocalNotStaticFinall")
        private final ThreadLocal<StatisticsData> threadData;
        private final Set<StatisticsDataReference> allData;
        private static final ReferenceQueue<Thread> STATS_DATA_REF_QUEUE;
        private static final Thread STATS_DATA_CLEANER;

        static {
            STATS_DATA_REF_QUEUE=new ReferenceQueue<>();
            STATS_DATA_CLEANER=new Thread(new StatisticsDataReferenceCleaner());
            STATS_DATA_CLEANER.setName(StatisticsDataReference.class.getName());
            STATS_DATA_CLEANER.setDaemon(true);
            STATS_DATA_CLEANER.start();
        }

        public Statistics(String scheme){
            this.scheme=scheme;
            this.rootData=new StatisticsData();
            this.threadData=new ThreadLocal<>();
            this.allData=new HashSet<>();
        }

        public Statistics(Statistics o){
            this.scheme=o.scheme;
            this.rootData=o.rootData;
            o.visitAll(new StatisticsAggregator<Void>(){
                @Override
                public void accept(StatisticsData data) {
                    rootData.add(data);
                }

                @Override
                public Void aggregate() {
                    return null;
                }
            });
            this.threadData=new ThreadLocal<>();
            this.allData=new HashSet<>();
        }

        public StatisticsData getThreadStatistics(){
            StatisticsData data = threadData.get();
            if (data == null) {
                data=new StatisticsData();
                threadData.set(data);
                StatisticsDataReference ref = new StatisticsDataReference(data, Thread.currentThread());
                synchronized (this) {
                    allData.add(ref);
                }
            }
            return data;
        }

        public void incrementBytesRead(long newBytes){
            getThreadStatistics().bytesRead+=newBytes;
        }
        public void incrementBytesWritten(long newBytes) {
            getThreadStatistics().bytesWritten += newBytes;
        }

        /**
         * Increment the number of read operations.
         * @param count number of read operations
         */
        public void incrementReadOps(int count) {
            getThreadStatistics().readOps += count;
        }

        /**
         * Increment the number of large read operations.
         * @param count number of large read operations
         */
        public void incrementLargeReadOps(int count) {
            getThreadStatistics().largeReadOps += count;
        }

        /**
         * Increment the number of write operations.
         * @param count number of write operations
         */
        public void incrementWriteOps(int count) {
            getThreadStatistics().writeOps += count;
        }

        /**
         * Increment the bytes read on erasure-coded files in the statistics.
         * @param newBytes the additional bytes read
         */
        public void incrementBytesReadErasureCoded(long newBytes) {
            getThreadStatistics().bytesReadErasureCoded += newBytes;
        }
        public void incrementBytesReadByDistance(int distance, long newBytes) {
            switch (distance) {
                case 0:
                    getThreadStatistics().bytesReadLocalHost += newBytes;
                    break;
                case 1:
                case 2:
                    getThreadStatistics().bytesReadDistanceOfOneOrTwo += newBytes;
                    break;
                case 3:
                case 4:
                    getThreadStatistics().bytesReadDistanceOfThreeOrFour += newBytes;
                    break;
                default:
                    getThreadStatistics().bytesReadDistanceOfFiveOrLarger += newBytes;
                    break;
            }
        }
        private synchronized <T> T visitAll(StatisticsAggregator<T> visitor){
            visitor.accept(rootData);
            for (StatisticsDataReference ref : allData) {
                StatisticsData data = ref.getData();
                visitor.accept(data);
            }
            return visitor.aggregate();
        }
        public long getBytesRead() {
            return visitAll(new StatisticsAggregator<Long>() {
                private long bytesRead = 0;

                @Override
                public void accept(StatisticsData data) {
                    bytesRead += data.bytesRead;
                }

                public Long aggregate() {
                    return bytesRead;
                }
            });
        }

        /**
         * Get the total number of bytes written.
         * @return the number of bytes
         */
        public long getBytesWritten() {
            return visitAll(new StatisticsAggregator<Long>() {
                private long bytesWritten = 0;

                @Override
                public void accept(StatisticsData data) {
                    bytesWritten += data.bytesWritten;
                }

                public Long aggregate() {
                    return bytesWritten;
                }
            });
        }

        /**
         * Get the number of file system read operations such as list files.
         * @return number of read operations
         */
        public int getReadOps() {
            return visitAll(new StatisticsAggregator<Integer>() {
                private int readOps = 0;

                @Override
                public void accept(StatisticsData data) {
                    readOps += data.readOps;
                    readOps += data.largeReadOps;
                }

                public Integer aggregate() {
                    return readOps;
                }
            });
        }

        /**
         * Get the number of large file system read operations such as list files
         * under a large directory.
         * @return number of large read operations
         */
        public int getLargeReadOps() {
            return visitAll(new StatisticsAggregator<Integer>() {
                private int largeReadOps = 0;

                @Override
                public void accept(StatisticsData data) {
                    largeReadOps += data.largeReadOps;
                }

                public Integer aggregate() {
                    return largeReadOps;
                }
            });
        }

        /**
         * Get the number of file system write operations such as create, append
         * rename etc.
         * @return number of write operations
         */
        public int getWriteOps() {
            return visitAll(new StatisticsAggregator<Integer>() {
                private int writeOps = 0;

                @Override
                public void accept(StatisticsData data) {
                    writeOps += data.writeOps;
                }

                public Integer aggregate() {
                    return writeOps;
                }
            });
        }

        /**
         * In the common network topology setup, distance value should be an even
         * number such as 0, 2, 4, 6. To make it more general, we group distance
         * by {1, 2}, {3, 4} and {5 and beyond} for accounting. So if the caller
         * ask for bytes read for distance 2, the function will return the value
         * for group {1, 2}.
         * @param distance the network distance
         * @return the total number of bytes read by the network distance
         */
        public long getBytesReadByDistance(int distance) {
            long bytesRead;
            switch (distance) {
                case 0:
                    bytesRead = getData().getBytesReadLocalHost();
                    break;
                case 1:
                case 2:
                    bytesRead = getData().getBytesReadDistanceOfOneOrTwo();
                    break;
                case 3:
                case 4:
                    bytesRead = getData().getBytesReadDistanceOfThreeOrFour();
                    break;
                default:
                    bytesRead = getData().getBytesReadDistanceOfFiveOrLarger();
                    break;
            }
            return bytesRead;
        }

        /**
         * Get all statistics data.
         * MR or other frameworks can use the method to get all statistics at once.
         * @return the StatisticsData
         */
        public StatisticsData getData() {
            return visitAll(new StatisticsAggregator<StatisticsData>() {
                private StatisticsData all = new StatisticsData();

                @Override
                public void accept(StatisticsData data) {
                    all.add(data);
                }

                public StatisticsData aggregate() {
                    return all;
                }
            });
        }

        /**
         * Get the total number of bytes read on erasure-coded files.
         * @return the number of bytes
         */
        public long getBytesReadErasureCoded() {
            return visitAll(new StatisticsAggregator<Long>() {
                private long bytesReadErasureCoded = 0;

                @Override
                public void accept(StatisticsData data) {
                    bytesReadErasureCoded += data.bytesReadErasureCoded;
                }

                public Long aggregate() {
                    return bytesReadErasureCoded;
                }
            });
        }
        @Override
        public String toString() {
            return visitAll(new StatisticsAggregator<String>() {
                private StatisticsData total = new StatisticsData();

                @Override
                public void accept(StatisticsData data) {
                    total.add(data);
                }

                public String aggregate() {
                    return total.toString();
                }
            });
        }
        public void reset() {
            visitAll(new StatisticsAggregator<Void>() {
                private StatisticsData total = new StatisticsData();

                @Override
                public void accept(StatisticsData data) {
                    total.add(data);
                }

                public Void aggregate() {
                    total.negate();
                    rootData.add(total);
                    return null;
                }
            });
        }

        /**
         * Get the uri scheme associated with this statistics object.
         * @return the schema associated with this set of statistics
         */
        public String getScheme() {
            return scheme;
        }

        @VisibleForTesting
        synchronized int getAllThreadLocalDataSize() {
            return allData.size();
        }

        public static class StatisticsData{
            private volatile long bytesRead;
            private volatile  long bytesWritten;
            private volatile int readOps;
            private volatile int largeReadOps;
            private volatile int writeOps;
            private volatile long bytesReadLocalHost;
            private volatile long bytesReadDistanceOfOneOrTwo;
            private volatile long bytesReadDistanceOfThreeOrFour;
            private volatile long bytesReadDistanceOfFiveOrLarger;
            private volatile long bytesReadErasureCoded;

            void add(StatisticsData other){
                this.bytesRead+=other.bytesRead;
                this.bytesWritten+=other.bytesWritten;
                this.readOps+= other.readOps;
                this.largeReadOps+=other.largeReadOps;
                this.writeOps+=other.writeOps;
                this.bytesReadLocalHost+=other.bytesReadLocalHost;
                this.bytesReadDistanceOfOneOrTwo+=other.bytesReadDistanceOfOneOrTwo;
                this.bytesReadDistanceOfThreeOrFour+=other.bytesReadDistanceOfThreeOrFour;
                this.bytesReadDistanceOfFiveOrLarger+=other.bytesReadDistanceOfFiveOrLarger;
                this.bytesReadErasureCoded+=other.bytesReadErasureCoded;
            }

            void negate(){
                this.bytesRead=-this.bytesRead;
                this.bytesWritten=-this.bytesWritten;
                this.readOps= -this.readOps;
                this.largeReadOps=-this.largeReadOps;
                this.writeOps=-this.writeOps;
                this.bytesReadLocalHost=-this.bytesReadLocalHost;
                this.bytesReadDistanceOfOneOrTwo=-this.bytesReadDistanceOfOneOrTwo;
                this.bytesReadDistanceOfThreeOrFour=-this.bytesReadDistanceOfThreeOrFour;
                this.bytesReadDistanceOfFiveOrLarger=-this.bytesReadDistanceOfFiveOrLarger;
                this.bytesReadErasureCoded=-this.bytesReadErasureCoded;
            }
            @Override
            public String toString() {
                return bytesRead + " bytes read, " + bytesWritten + " bytes written, "
                        + readOps + " read ops, " + largeReadOps + " large read ops, "
                        + writeOps + " write ops";
            }

            public long getBytesRead() {
                return bytesRead;
            }

            public long getBytesWritten() {
                return bytesWritten;
            }

            public int getReadOps() {
                return readOps;
            }

            public int getLargeReadOps() {
                return largeReadOps;
            }

            public int getWriteOps() {
                return writeOps;
            }

            public long getBytesReadLocalHost() {
                return bytesReadLocalHost;
            }

            public long getBytesReadDistanceOfOneOrTwo() {
                return bytesReadDistanceOfOneOrTwo;
            }

            public long getBytesReadDistanceOfThreeOrFour() {
                return bytesReadDistanceOfThreeOrFour;
            }

            public long getBytesReadDistanceOfFiveOrLarger() {
                return bytesReadDistanceOfFiveOrLarger;
            }

            public long getBytesReadErasureCoded() {
                return bytesReadErasureCoded;
            }
        }
        private interface StatisticsAggregator<T>{
            void accept(StatisticsData data);
            T aggregate();
        }
        private final class StatisticsDataReference extends WeakReference<Thread>{
            private final StatisticsData data;
            private StatisticsDataReference(StatisticsData data,Thread thread){
                super(thread,STATS_DATA_REF_QUEUE);
                this.data=data;
            }

            public StatisticsData getData() {
                return data;
            }
            public void cleanUp(){
                synchronized (Statistics.this){
                    rootData.add(data);
                    allData.remove(this);
                }
            }
        }
        private static class StatisticsDataReferenceCleaner implements Runnable{
            @Override
            public void run() {
                while (!Thread.interrupted()) {
                    try {
                        StatisticsDataReference ref=(StatisticsDataReference) STATS_DATA_REF_QUEUE.remove();
                        ref.cleanUp();
                    }catch (InterruptedException e){
                        LOGGER.warn("Cleaner thread interrupted, will stop",e);
                        Thread.currentThread().interrupt();
                    }catch (Throwable e){
                        LOGGER.warn("Exception in the cleaner thread but it will continue to run",e);
                    }
                }
            }
        }
    }
    private static class FSDataInputStreamBuilder extends FutureDataInputStreamBuilderImpl
            implements FutureDataInputStreamBuilder{
        protected FSDataInputStreamBuilder(@Nonnull final FileSystem fileSystem,
                                           @Nonnull final Path path){
            super(fileSystem,path);
        }
        protected FSDataInputStreamBuilder(@Nonnull final FileSystem fileSystem,
                                           @Nonnull final PathHandler pathHandler){
            super(fileSystem,pathHandler);
        }

        @Override
        public CompletableFuture<FSDataInputStream> build() throws IllegalArgumentException, UnsupportedOperationException, IOException {
            Optional<Path> optionalPath = getOptionalPath();
            OpenFileParameters parameters=new OpenFileParameters()
                    .wi
        }
    }
}
