/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.segment.realtime.plumber;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Granularity;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.client.CachingQueryRunner;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.common.guava.ThreadRenamingCallable;
import io.druid.common.guava.ThreadRenamingRunnable;
import io.druid.common.utils.VMUtils;
import io.druid.concurrent.Execs;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.query.MetricsEmittingQueryRunner;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryToolChest;
import io.druid.query.ReportTimelineMissingSegmentQueryRunner;
import io.druid.query.SegmentDescriptor;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMaker;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.ReferenceCountingSegment;
import io.druid.segment.Segment;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.FireHydrant;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.SingleElementPartitionChunk;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 */
public class RealtimePlumber implements Plumber
{
  private static final EmittingLogger log = new EmittingLogger(RealtimePlumber.class);
  private static final int WARN_DELAY = 1000;

  private final DataSchema schema;
  private final RealtimeTuningConfig config;
  private final RejectionPolicy rejectionPolicy;
  private final FireDepartmentMetrics metrics;
  private final ServiceEmitter emitter;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final ExecutorService queryExecutorService;
  private final DataSegmentPusher dataSegmentPusher;
  private final SegmentPublisher segmentPublisher;
  private final SegmentHandoffNotifier handoffNotifier;
  private final Object handoffCondition = new Object();
  private final Map<Long, Sink> sinks = Maps.newConcurrentMap();
  private final VersionedIntervalTimeline<String, Sink> sinkTimeline = new VersionedIntervalTimeline<String, Sink>(
      String.CASE_INSENSITIVE_ORDER
  );

  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final ObjectMapper objectMapper;

  private volatile long nextFlush = 0;
  private volatile boolean shuttingDown = false;
  private volatile boolean stopped = false;
  private volatile boolean cleanShutdown = true;
  private volatile ExecutorService persistExecutor = null;
  private volatile ExecutorService mergeExecutor = null;
  private volatile ScheduledExecutorService scheduledExecutor = null;

  private static final String COMMIT_METADATA_KEY = "%commitMetadata%";
  private static final String COMMIT_METADATA_TIMESTAMP_KEY = "%commitMetadataTimestamp%";
  private static final String SKIP_INCREMENTAL_SEGMENT = "skipIncrementalSegment";


  public RealtimePlumber(
      DataSchema schema,
      RealtimeTuningConfig config,
      FireDepartmentMetrics metrics,
      ServiceEmitter emitter,
      QueryRunnerFactoryConglomerate conglomerate,
      DataSegmentAnnouncer segmentAnnouncer,
      ExecutorService queryExecutorService,
      DataSegmentPusher dataSegmentPusher,
      SegmentPublisher segmentPublisher,
      SegmentHandoffNotifier handoffNotifier,
      Cache cache,
      CacheConfig cacheConfig,
      ObjectMapper objectMapper
  )
  {
    this.schema = schema;
    this.config = config;
    this.rejectionPolicy = config.getRejectionPolicyFactory().create(config.getWindowPeriod());
    this.metrics = metrics;
    this.emitter = emitter;
    this.conglomerate = conglomerate;
    this.segmentAnnouncer = segmentAnnouncer;
    this.queryExecutorService = queryExecutorService;
    this.dataSegmentPusher = dataSegmentPusher;
    this.segmentPublisher = segmentPublisher;
    this.handoffNotifier = handoffNotifier;
    this.cache = cache;
    this.cacheConfig = cacheConfig;
    this.objectMapper = objectMapper;

    if (!cache.isLocal()) {
      log.error("Configured cache is not local, caching will not be enabled");
    }

    log.info("Creating plumber using rejectionPolicy[%s]", getRejectionPolicy());
  }

  public DataSchema getSchema()
  {
    return schema;
  }

  public RealtimeTuningConfig getConfig()
  {
    return config;
  }

  public RejectionPolicy getRejectionPolicy()
  {
    return rejectionPolicy;
  }

  public Map<Long, Sink> getSinks()
  {
    return sinks;
  }

  @Override
  public Object startJob()
  {
    computeBaseDir(schema).mkdirs();
    initializeExecutors();
    handoffNotifier.start();
    Object retVal = bootstrapSinksFromDisk();
    startPersistThread();
    // Push pending sinks bootstrapped from previous run
    mergeAndPush();
    resetNextFlush();
    return retVal;
  }

  @Override
  public int add(InputRow row, Supplier<Committer> committerSupplier) throws IndexSizeExceededException
  {
    final Sink sink = getSink(row.getTimestampFromEpoch());
    if (sink == null) {
      return -1;
    }

    final int numRows = sink.add(row);

    if (!sink.canAppendRow() || System.currentTimeMillis() > nextFlush) {
      persist(committerSupplier.get());
    }

    return numRows;
  }

  private Sink getSink(long timestamp)
  {
    if (!rejectionPolicy.accept(timestamp)) {
      return null;
    }

    final Granularity segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    final VersioningPolicy versioningPolicy = config.getVersioningPolicy();

    final long truncatedTime = segmentGranularity.truncate(new DateTime(timestamp)).getMillis();

    Sink retVal = sinks.get(truncatedTime);

    if (retVal == null) {
      final Interval sinkInterval = new Interval(
          new DateTime(truncatedTime),
          segmentGranularity.increment(new DateTime(truncatedTime))
      );

      retVal = new Sink(sinkInterval, schema, config, versioningPolicy.getVersion(sinkInterval));
      addSink(retVal);

    }

    return retVal;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(final Query<T> query)
  {
    final boolean skipIncrementalSegment = query.getContextValue(SKIP_INCREMENTAL_SEGMENT, false);
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    final QueryToolChest<T, Query<T>> toolchest = factory.getToolchest();

    final Function<Query<T>, ServiceMetricEvent.Builder> builderFn =
        new Function<Query<T>, ServiceMetricEvent.Builder>()
        {

          @Override
          public ServiceMetricEvent.Builder apply(@Nullable Query<T> input)
          {
            return toolchest.makeMetricBuilder(query);
          }
        };

    List<TimelineObjectHolder<String, Sink>> querySinks = Lists.newArrayList();
    for (Interval interval : query.getIntervals()) {
      querySinks.addAll(sinkTimeline.lookup(interval));
    }

    return toolchest.mergeResults(
        factory.mergeRunners(
            queryExecutorService,
            FunctionalIterable
                .create(querySinks)
                .transform(
                    new Function<TimelineObjectHolder<String, Sink>, QueryRunner<T>>()
                    {
                      @Override
                      public QueryRunner<T> apply(TimelineObjectHolder<String, Sink> holder)
                      {
                        if (holder == null) {
                          throw new ISE("No timeline entry at all!");
                        }

                        // The realtime plumber always uses SingleElementPartitionChunk
                        final Sink theSink = holder.getObject().getChunk(0).getObject();
                        final boolean skipIncrementalSegment = query.getContextValue(SKIP_INCREMENTAL_SEGMENT, false);

                        if (theSink == null) {
                          throw new ISE("Missing sink for timeline entry[%s]!", holder);
                        }

                        final SegmentDescriptor descriptor = new SegmentDescriptor(
                            holder.getInterval(),
                            theSink.getSegment().getVersion(),
                            theSink.getSegment().getShardSpec().getPartitionNum()
                        );

                        return new SpecificSegmentQueryRunner<T>(
                            new MetricsEmittingQueryRunner<T>(
                                emitter,
                                builderFn,
                                factory.mergeRunners(
                                    MoreExecutors.sameThreadExecutor(),
                                    Iterables.transform(
                                        theSink,
                                        new Function<FireHydrant, QueryRunner<T>>()
                                        {
                                          @Override
                                          public QueryRunner<T> apply(FireHydrant input)
                                          {
                                            // It is possible that we got a query for a segment, and while that query
                                            // is in the jetty queue, the segment is abandoned. Here, we need to retry
                                            // the query for the segment.
                                            if (input == null || input.getSegment() == null) {
                                              return new ReportTimelineMissingSegmentQueryRunner<T>(descriptor);
                                            }

                                            if (skipIncrementalSegment && !input.hasSwapped()) {
                                              return new NoopQueryRunner<T>();
                                            }

                                            // Prevent the underlying segment from closing when its being iterated
                                            final ReferenceCountingSegment segment = input.getSegment();
                                            final Closeable closeable = segment.increment();
                                            try {
                                              if (input.hasSwapped() // only use caching if data is immutable
                                                  && cache.isLocal() // hydrants may not be in sync between replicas, make sure cache is local
                                                  ) {
                                                return new CachingQueryRunner<>(
                                                    makeHydrantIdentifier(input, segment),
                                                    descriptor,
                                                    objectMapper,
                                                    cache,
                                                    toolchest,
                                                    factory.createRunner(segment),
                                                    MoreExecutors.sameThreadExecutor(),
                                                    cacheConfig
                                                );
                                              } else {
                                                return factory.createRunner(input.getSegment());
                                              }
                                            }
                                            finally {
                                              try {
                                                if (closeable != null) {
                                                  closeable.close();
                                                }
                                              }
                                              catch (IOException e) {
                                                throw Throwables.propagate(e);
                                              }
                                            }
                                          }
                                        }
                                    )
                                )
                            ).withWaitMeasuredFromNow(),
                            new SpecificSegmentSpec(
                                descriptor
                            )
                        );
                      }
                    }
                )
        )
    );
  }

  protected static String makeHydrantIdentifier(FireHydrant input, ReferenceCountingSegment segment)
  {
    return segment.getIdentifier() + "_" + input.getCount();
  }

  @Override
  public void persist(final Committer committer)
  {
    final List<Pair<FireHydrant, Interval>> indexesToPersist = Lists.newArrayList();
    for (Sink sink : sinks.values()) {
      if (sink.swappable()) {
        indexesToPersist.add(Pair.of(sink.swap(), sink.getInterval()));
      }
    }

    log.info("Submitting persist runnable for dataSource[%s]", schema.getDataSource());

    final Stopwatch runExecStopwatch = Stopwatch.createStarted();
    final Stopwatch persistStopwatch = Stopwatch.createStarted();

    final Map<String, Object> metadata = committer.getMetadata() == null ? null :
                                         ImmutableMap.of(
                                             COMMIT_METADATA_KEY,
                                             committer.getMetadata(),
                                             COMMIT_METADATA_TIMESTAMP_KEY,
                                             System.currentTimeMillis()
                                         );

    persistExecutor.execute(
        new ThreadRenamingRunnable(String.format("%s-incremental-persist", schema.getDataSource()))
        {
          @Override
          public void doRun()
          {
            /* Note:
            If plumber crashes after storing a subset of all the hydrants then we will lose data and next
            time we will start with the commitMetadata stored in those hydrants.
            option#1:
            maybe it makes sense to store the metadata outside the segments in a separate file. This is because the
            commit metadata isn't really associated with an individual segment-- it's associated with a set of segments
            that are persisted at the same time or maybe whole datasource. So storing it in the segments is asking for problems.
            Sort of like this:

            {
              "metadata" : {"foo": "bar"},
              "segments": [
                {"id": "datasource_2000_2001_2000_1", "hydrant": 10},
                {"id": "datasource_2001_2002_2001_1", "hydrant": 12},
              ]
            }
            When a realtime node crashes and starts back up, it would delete any hydrants numbered higher than the
            ones in the commit file.

            option#2
            We could also just include the set of segments for the same chunk of metadata in more metadata on each
            of the segments. we might also have to think about the hand-off in terms of the full set of segments being
            handed off instead of individual segments being handed off (that is, if one of the set succeeds in handing
            off and the others fail, the real-time would believe that it needs to re-ingest the data).
             */
            long persistThreadCpuTime = VMUtils.safeGetThreadCpuTime();
            try {
              for (Pair<FireHydrant, Interval> pair : indexesToPersist) {
                metrics.incrementRowOutputCount(
                    persistHydrant(
                        pair.lhs, schema, pair.rhs, metadata
                    )
                );
              }
              committer.run();
            }
            catch (Exception e) {
              metrics.incrementFailedPersists();
              throw e;
            }
            finally {
              metrics.incrementPersistCpuTime(VMUtils.safeGetThreadCpuTime() - persistThreadCpuTime);
              metrics.incrementNumPersists();
              metrics.incrementPersistTimeMillis(persistStopwatch.elapsed(TimeUnit.MILLISECONDS));
              persistStopwatch.stop();
            }
          }
        }
    );

    final long startDelay = runExecStopwatch.elapsed(TimeUnit.MILLISECONDS);
    metrics.incrementPersistBackPressureMillis(startDelay);
    if (startDelay > WARN_DELAY) {
      log.warn("Ingestion was throttled for [%,d] millis because persists were pending.", startDelay);
    }
    runExecStopwatch.stop();
    resetNextFlush();
  }

  // Submits persist-n-merge task for a Sink to the mergeExecutor
  private void persistAndMerge(final long truncatedTime, final Sink sink)
  {
    final String threadName = String.format(
        "%s-%s-persist-n-merge", schema.getDataSource(), new DateTime(truncatedTime)
    );
    mergeExecutor.execute(
        new ThreadRenamingRunnable(threadName)
        {
          @Override
          public void doRun()
          {
            final Interval interval = sink.getInterval();

            // Bail out if this sink has been abandoned by a previously-executed task.
            if (sinks.get(truncatedTime) != sink) {
              log.info("Sink[%s] was abandoned, bailing out of persist-n-merge.", sink);
              return;
            }

            // Use a file to indicate that pushing has completed.
            final File persistDir = computePersistDir(schema, interval);
            final File mergedTarget = new File(persistDir, "merged");
            final File isPushedMarker = new File(persistDir, "isPushedMarker");

            if (!isPushedMarker.exists()) {
              removeSegment(sink, mergedTarget);
              if (mergedTarget.exists()) {
                log.wtf("Merged target[%s] exists?!", mergedTarget);
                return;
              }
            } else {
              log.info("Already pushed sink[%s]", sink);
              return;
            }

            /*
            Note: it the plumber crashes after persisting a subset of hydrants then might duplicate data as these
            hydrants will be read but older commitMetadata will be used. fixing this possibly needs structural
            changes to plumber.
             */
            for (FireHydrant hydrant : sink) {
              synchronized (hydrant) {
                if (!hydrant.hasSwapped()) {
                  log.info("Hydrant[%s] hasn't swapped yet, swapping. Sink[%s]", hydrant, sink);
                  final int rowCount = persistHydrant(hydrant, schema, interval, null);
                  metrics.incrementRowOutputCount(rowCount);
                }
              }
            }
            final long mergeThreadCpuTime = VMUtils.safeGetThreadCpuTime();
            final Stopwatch mergeStopwatch = Stopwatch.createStarted();
            try {
              List<QueryableIndex> indexes = Lists.newArrayList();
              for (FireHydrant fireHydrant : sink) {
                Segment segment = fireHydrant.getSegment();
                final QueryableIndex queryableIndex = segment.asQueryableIndex();
                log.info("Adding hydrant[%s]", fireHydrant);
                indexes.add(queryableIndex);
              }

              final File mergedFile;
              if (config.isPersistInHeap()) {
                mergedFile = IndexMaker.mergeQueryableIndex(
                    indexes,
                    schema.getAggregators(),
                    mergedTarget,
                    config.getIndexSpec()
                );
              } else {
                mergedFile = IndexMerger.mergeQueryableIndex(
                    indexes,
                    schema.getAggregators(),
                    mergedTarget,
                    config.getIndexSpec()
                );
              }
              // emit merge metrics before publishing segment
              metrics.incrementMergeCpuTime(VMUtils.safeGetThreadCpuTime() - mergeThreadCpuTime);
              metrics.incrementMergeTimeMillis(mergeStopwatch.elapsed(TimeUnit.MILLISECONDS));

              QueryableIndex index = IndexIO.loadIndex(mergedFile);
              log.info("Pushing [%s] to deep storage", sink.getSegment().getIdentifier());

              DataSegment segment = dataSegmentPusher.push(
                  mergedFile,
                  sink.getSegment().withDimensions(Lists.newArrayList(index.getAvailableDimensions()))
              );
              log.info("Inserting [%s] to the metadata store", sink.getSegment().getIdentifier());
              segmentPublisher.publishSegment(segment);

              if (!isPushedMarker.createNewFile()) {
                log.makeAlert("Failed to create marker file for [%s]", schema.getDataSource())
                   .addData("interval", sink.getInterval())
                   .addData("partitionNum", segment.getShardSpec().getPartitionNum())
                   .addData("marker", isPushedMarker)
                   .emit();
              }
            }
            catch (Exception e) {
              metrics.incrementFailedHandoffs();
              log.makeAlert(e, "Failed to persist merged index[%s]", schema.getDataSource())
                 .addData("interval", interval)
                 .emit();
              if (shuttingDown) {
                // We're trying to shut down, and this segment failed to push. Let's just get rid of it.
                // This call will also delete possibly-partially-written files, so we don't need to do it explicitly.
                cleanShutdown = false;
                abandonSegment(truncatedTime, sink);
              }
            }
            finally {
              mergeStopwatch.stop();
            }
          }
        }
    );
    handoffNotifier.registerSegmentHandoffCallback(
        new SegmentDescriptor(sink.getInterval(), sink.getVersion(), config.getShardSpec().getPartitionNum()),
        mergeExecutor, new Runnable()
        {
          @Override
          public void run()
          {
            abandonSegment(sink.getInterval().getStartMillis(), sink);
          }
        }
    );
  }

  @Override
  public void finishJob()
  {
    log.info("Shutting down...");

    shuttingDown = true;

    for (final Map.Entry<Long, Sink> entry : sinks.entrySet()) {
      persistAndMerge(entry.getKey(), entry.getValue());
    }

    while (!sinks.isEmpty()) {
      try {
        log.info(
            "Cannot shut down yet! Sinks remaining: %s",
            Joiner.on(", ").join(
                Iterables.transform(
                    sinks.values(),
                    new Function<Sink, String>()
                    {
                      @Override
                      public String apply(Sink input)
                      {
                        return input.getSegment().getIdentifier();
                      }
                    }
                )
            )
        );

        synchronized (handoffCondition) {
          while (!sinks.isEmpty()) {
            handoffCondition.wait();
          }
        }
      }
      catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }

    handoffNotifier.stop();
    shutdownExecutors();

    stopped = true;

    if (!cleanShutdown) {
      throw new ISE("Exception occurred during persist and merge.");
    }
  }

  private void resetNextFlush()
  {
    nextFlush = new DateTime().plus(config.getIntermediatePersistPeriod()).getMillis();
  }

  protected void initializeExecutors()
  {
    final int maxPendingPersists = config.getMaxPendingPersists();

    if (persistExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      persistExecutor = Execs.newBlockingSingleThreaded(
          "plumber_persist_%d", maxPendingPersists
      );
    }
    if (mergeExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      mergeExecutor = Execs.newBlockingSingleThreaded(
          "plumber_merge_%d", 1
      );
    }

    if (scheduledExecutor == null) {
      scheduledExecutor = Execs.scheduledSingleThreaded("plumber_scheduled_%d");
    }
  }

  protected void shutdownExecutors()
  {
    // scheduledExecutor is shutdown here
    if (scheduledExecutor != null) {
      scheduledExecutor.shutdown();
      persistExecutor.shutdown();
      mergeExecutor.shutdown();
    }
  }

  protected Object bootstrapSinksFromDisk()
  {
    final VersioningPolicy versioningPolicy = config.getVersioningPolicy();

    File baseDir = computeBaseDir(schema);
    if (baseDir == null || !baseDir.exists()) {
      return null;
    }

    File[] files = baseDir.listFiles();
    if (files == null) {
      return null;
    }

    Object metadata = null;
    long latestCommitTime = 0;
    for (File sinkDir : files) {
      final Interval sinkInterval = new Interval(sinkDir.getName().replace("_", "/"));

      //final File[] sinkFiles = sinkDir.listFiles();
      // To avoid reading and listing of "merged" dir
      final File[] sinkFiles = sinkDir.listFiles(
          new FilenameFilter()
          {
            @Override
            public boolean accept(File dir, String fileName)
            {
              return !(Ints.tryParse(fileName) == null);
            }
          }
      );
      Arrays.sort(
          sinkFiles,
          new Comparator<File>()
          {
            @Override
            public int compare(File o1, File o2)
            {
              try {
                return Ints.compare(Integer.parseInt(o1.getName()), Integer.parseInt(o2.getName()));
              }
              catch (NumberFormatException e) {
                log.error(e, "Couldn't compare as numbers? [%s][%s]", o1, o2);
                return o1.compareTo(o2);
              }
            }
          }
      );
      boolean isCorrupted = false;
      List<FireHydrant> hydrants = Lists.newArrayList();
      for (File segmentDir : sinkFiles) {
        log.info("Loading previously persisted segment at [%s]", segmentDir);

        // Although this has been tackled at start of this method.
        // Just a doubly-check added to skip "merged" dir. from being added to hydrants
        // If 100% sure that this is not needed, this check can be removed.
        if (Ints.tryParse(segmentDir.getName()) == null) {
          continue;
        }
        QueryableIndex queryableIndex = null;
        try {
          queryableIndex = IndexIO.loadIndex(segmentDir);
        }
        catch (IOException e) {
          log.error(e, "Problem loading segmentDir from disk.");
          isCorrupted = true;
        }
        if (isCorrupted) {
          try {
            queryableIndex = IndexIO.loadIndex(segmentDir);
          }
          catch (IOException e) {
            log.error(e, "Problem loading segmentDir from disk.");
            isCorrupted = true;
          }
          catch (Exception e1) {
            log.error(e1, "Failed to rename %s", segmentDir.getAbsolutePath());
          }
          //Note: skipping corrupted segment might lead to dropping some data. This strategy should be changed
          //at some point.
          continue;
        }
        Map<String, Object> segmentMetadata = queryableIndex.getMetaData();
        if (segmentMetadata != null) {
          Object timestampObj = segmentMetadata.get(COMMIT_METADATA_TIMESTAMP_KEY);
          if (timestampObj != null) {
            long timestamp = ((Long) timestampObj).longValue();
            if (timestamp > latestCommitTime) {
              log.info(
                  "Found metaData [%s] with latestCommitTime [%s] greater than previous recorded [%s]",
                  queryableIndex.getMetaData(), timestamp, latestCommitTime
              );
              latestCommitTime = timestamp;
              metadata = queryableIndex.getMetaData().get(COMMIT_METADATA_KEY);
            }
          }
        }
        hydrants.add(
            new FireHydrant(
                new QueryableIndexSegment(
                    DataSegment.makeDataSegmentIdentifier(
                        schema.getDataSource(),
                        sinkInterval.getStart(),
                        sinkInterval.getEnd(),
                        versioningPolicy.getVersion(sinkInterval),
                        config.getShardSpec()
                    ),
                    queryableIndex
                ),
                Integer.parseInt(segmentDir.getName())
            )
        );
      }
      if (hydrants.isEmpty()) {
        // Probably encountered a corrupt sink directory
        log.warn(
            "Found persisted segment directory with no intermediate segments present at %s, skipping sink creation.",
            sinkDir.getAbsolutePath()
        );
        continue;
      }
      final Sink currSink = new Sink(sinkInterval, schema, config, versioningPolicy.getVersion(sinkInterval), hydrants);
      addSink(currSink);
    }
    return metadata;
  }

  private void addSink(final Sink sink)
  {
    sinks.put(sink.getInterval().getStartMillis(), sink);
    sinkTimeline.add(
        sink.getInterval(),
        sink.getVersion(),
        new SingleElementPartitionChunk<Sink>(sink)
    );
    try {
      segmentAnnouncer.announceSegment(sink.getSegment());
    }
    catch (IOException e) {
      log.makeAlert(e, "Failed to announce new segment[%s]", schema.getDataSource())
         .addData("interval", sink.getInterval())
         .emit();
    }
  }

  protected void startPersistThread()
  {
    final Granularity segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    final Period windowPeriod = config.getWindowPeriod();

    final DateTime truncatedNow = segmentGranularity.truncate(new DateTime());
    final long windowMillis = windowPeriod.toStandardDuration().getMillis();

    log.info(
        "Expect to run at [%s]",
        new DateTime().plus(
            new Duration(
                System.currentTimeMillis(),
                segmentGranularity.increment(truncatedNow).getMillis() + windowMillis
            )
        )
    );

    ScheduledExecutors
        .scheduleAtFixedRate(
            scheduledExecutor,
            new Duration(
                System.currentTimeMillis(),
                segmentGranularity.increment(truncatedNow).getMillis() + windowMillis
            ),
            new Duration(truncatedNow, segmentGranularity.increment(truncatedNow)),
            new ThreadRenamingCallable<ScheduledExecutors.Signal>(
                String.format(
                    "%s-overseer-%d",
                    schema.getDataSource(),
                    config.getShardSpec().getPartitionNum()
                )
            )
            {
              @Override
              public ScheduledExecutors.Signal doCall()
              {
                if (stopped) {
                  log.info("Stopping merge-n-push overseer thread");
                  return ScheduledExecutors.Signal.STOP;
                }

                mergeAndPush();

                if (stopped) {
                  log.info("Stopping merge-n-push overseer thread");
                  return ScheduledExecutors.Signal.STOP;
                } else {
                  return ScheduledExecutors.Signal.REPEAT;
                }
              }
            }
        );
  }

  private void mergeAndPush()
  {
    final Granularity segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    final Period windowPeriod = config.getWindowPeriod();

    final long windowMillis = windowPeriod.toStandardDuration().getMillis();
    log.info("Starting merge and push.");
    DateTime minTimestampAsDate = segmentGranularity.truncate(
        new DateTime(
            Math.max(
                windowMillis,
                rejectionPolicy.getCurrMaxTime()
                               .getMillis()
            )
            - windowMillis
        )
    );
    long minTimestamp = minTimestampAsDate.getMillis();

    log.info(
        "Found [%,d] segments. Attempting to hand off segments that start before [%s].",
        sinks.size(),
        minTimestampAsDate
    );

    List<Map.Entry<Long, Sink>> sinksToPush = Lists.newArrayList();
    for (Map.Entry<Long, Sink> entry : sinks.entrySet()) {
      final Long intervalStart = entry.getKey();
      if (intervalStart < minTimestamp) {
        log.info("Adding entry [%s] for merge and push.", entry);
        sinksToPush.add(entry);
      } else {
        log.info(
            "Skipping persist and merge for entry [%s] : Start time [%s] >= [%s] min timestamp required in this run. Segment will be picked up in a future run.",
            entry,
            new DateTime(intervalStart),
            minTimestampAsDate
        );
      }
    }

    log.info("Found [%,d] sinks to persist and merge", sinksToPush.size());

    for (final Map.Entry<Long, Sink> entry : sinksToPush) {
      persistAndMerge(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Unannounces a given sink and removes all local references to it. It is important that this is only called
   * from the single-threaded mergeExecutor, since otherwise chaos may ensue if merged segments are deleted while
   * being created.
   *
   * @param truncatedTime sink key
   * @param sink          sink to unannounce
   */
  protected void abandonSegment(final long truncatedTime, final Sink sink)
  {
    if (sinks.containsKey(truncatedTime)) {
      try {
        segmentAnnouncer.unannounceSegment(sink.getSegment());
        removeSegment(sink, computePersistDir(schema, sink.getInterval()));
        log.info("Removing sinkKey %d for segment %s", truncatedTime, sink.getSegment().getIdentifier());
        sinks.remove(truncatedTime);
        sinkTimeline.remove(
            sink.getInterval(),
            sink.getVersion(),
            new SingleElementPartitionChunk<>(sink)
        );
        for (FireHydrant hydrant : sink) {
          cache.close(makeHydrantIdentifier(hydrant, hydrant.getSegment()));
        }
        synchronized (handoffCondition) {
          handoffCondition.notifyAll();
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Unable to abandon old segment for dataSource[%s]", schema.getDataSource())
           .addData("interval", sink.getInterval())
           .emit();
      }
    }
  }

  protected File computeBaseDir(DataSchema schema)
  {
    return new File(config.getBasePersistDirectory(), schema.getDataSource());
  }

  protected File computeCorruptedFileDumpDir(File persistDir, DataSchema schema)
  {
    return new File(
        persistDir.getAbsolutePath()
                  .replace(schema.getDataSource(), "corrupted" + File.pathSeparator + schema.getDataSource())
    );
  }

  protected File computePersistDir(DataSchema schema, Interval interval)
  {
    return new File(computeBaseDir(schema), interval.toString().replace("/", "_"));
  }

  /**
   * Persists the given hydrant and returns the number of rows persisted
   *
   * @param indexToPersist hydrant to persist
   * @param schema         datasource schema
   * @param interval       interval to persist
   *
   * @return the number of rows persisted
   */
  protected int persistHydrant(
      FireHydrant indexToPersist,
      DataSchema schema,
      Interval interval,
      Map<String, Object> metaData
  )
  {
    synchronized (indexToPersist) {
      if (indexToPersist.hasSwapped()) {
        log.info(
            "DataSource[%s], Interval[%s], Hydrant[%s] already swapped. Ignoring request to persist.",
            schema.getDataSource(), interval, indexToPersist
        );
        return 0;
      }

      log.info(
          "DataSource[%s], Interval[%s], Metadata [%s] persisting Hydrant[%s]",
          schema.getDataSource(),
          interval,
          metaData,
          indexToPersist
      );
      try {
        int numRows = indexToPersist.getIndex().size();

        final File persistedFile;
        final IndexSpec indexSpec = config.getIndexSpec();

        if (config.isPersistInHeap()) {
          persistedFile = IndexMaker.persist(
              indexToPersist.getIndex(),
              new File(computePersistDir(schema, interval), String.valueOf(indexToPersist.getCount())),
              metaData,
              indexSpec
          );
        } else {
          persistedFile = IndexMerger.persist(
              indexToPersist.getIndex(),
              new File(computePersistDir(schema, interval), String.valueOf(indexToPersist.getCount())),
              metaData,
              indexSpec
          );
        }

        indexToPersist.swapSegment(
            new QueryableIndexSegment(
                indexToPersist.getSegment().getIdentifier(),
                IndexIO.loadIndex(persistedFile)
            )
        );
        return numRows;
      }
      catch (IOException e) {
        log.makeAlert("dataSource[%s] -- incremental persist failed", schema.getDataSource())
           .addData("interval", interval)
           .addData("count", indexToPersist.getCount())
           .emit();

        throw Throwables.propagate(e);
      }
    }
  }

  private void removeSegment(final Sink sink, final File target)
  {
    if (target.exists()) {
      try {
        log.info("Deleting Index File[%s]", target);
        FileUtils.deleteDirectory(target);
      }
      catch (Exception e) {
        log.makeAlert(e, "Unable to remove file for dataSource[%s]", schema.getDataSource())
           .addData("file", target)
           .addData("interval", sink.getInterval())
           .emit();
      }
    }
  }
}
