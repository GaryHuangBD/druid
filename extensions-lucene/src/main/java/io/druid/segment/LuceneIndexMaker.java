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

package io.druid.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.SerializerUtils;
import io.druid.guice.GuiceInjectors;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.codes.WhaledbCodec;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.LuceneDocumentTransformer;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 */
public class LuceneIndexMaker
{
  private static final Logger log = new Logger(LuceneIndexMaker.class);
  private static final SerializerUtils serializerUtils = new SerializerUtils();
  private static final int INVALID_ROW = -1;
  private static final Splitter SPLITTER = Splitter.on(",");
  private static final ObjectMapper mapper;

  static {
    final Injector injector = GuiceInjectors.makeStartupInjectorWithModules(ImmutableList.<Module>of());
    mapper = injector.getInstance(ObjectMapper.class);
  }

  public static File persist(
      final IncrementalIndex index,
      File outDir,
      final Map<String, Object> segmentMetadata,
      final IndexSpec indexSpec
  ) throws IOException
  {
    return persist(index, index.getInterval(), outDir, segmentMetadata, indexSpec);
  }

  /**
   * This is *not* thread-safe and havok will ensue if this is called and writes are still occurring
   * on the IncrementalIndex object.
   *
   * @param index        the IncrementalIndex to persist
   * @param dataInterval the Interval that the data represents
   * @param outDir       the directory to persist the data to
   *
   * @throws IOException
   */
  public static File persist(
      final IncrementalIndex index,
      final Interval dataInterval,
      File outDir,
      final Map<String, Object> segmentMetadata,
      final IndexSpec indexSpec
  ) throws IOException
  {
    return persist(
        index, dataInterval, outDir, segmentMetadata, indexSpec, new LoggingProgressIndicator(outDir.toString())
    );
  }

  public static File persist(
      final IncrementalIndex index,
      final Interval dataInterval,
      File outDir,
      final Map<String, Object> segmentMetadata,
      final IndexSpec indexSpec,
      ProgressIndicator progress
  ) throws IOException
  {
    if (index.isEmpty()) {
      throw new IAE("Trying to persist an empty index!");
    }

    final long firstTimestamp = index.getMinTime().getMillis();
    final long lastTimestamp = index.getMaxTime().getMillis();
    if (!(dataInterval.contains(firstTimestamp) && dataInterval.contains(lastTimestamp))) {
      throw new IAE(
          "interval[%s] does not encapsulate the full range of timestamps[%s, %s]",
          dataInterval,
          new DateTime(firstTimestamp),
          new DateTime(lastTimestamp)
      );
    }

    if (!outDir.exists()) {
      outDir.mkdirs();
    }
    if (!outDir.isDirectory()) {
      throw new ISE("Can only persist to directories, [%s] wasn't a directory", outDir);
    }

    log.info("Starting persist for interval[%s], rows[%,d]", dataInterval, index.size());
    FileUtils.deleteDirectory(outDir);
    if (!outDir.mkdirs()) {
      throw new ISE("Couldn't make outdir[%s].", outDir);
    }

    LuceneDocumentTransformer transformer = new LuceneDocumentTransformer(index);
    Directory dir = new NIOFSDirectory(Paths.get(outDir.toURI()));
    IndexWriterConfig iwc = new IndexWriterConfig(new SimpleAnalyzer());
    // for test
    iwc.setUseCompoundFile(false);
    WhaledbCodec.setCodes(iwc);
    IndexWriter writer = new IndexWriter(dir, iwc);
    writer.addDocuments(transformer.transform());
    writer.close();
    return outDir;
  }

  public static File mergeQueryableIndex(
      List<LuceneQueryableIndex> indexes, final AggregatorFactory[] metricAggs, File outDir, final IndexSpec indexSpec
  ) throws IOException
  {
    return mergeQueryableIndex(indexes, metricAggs, outDir, indexSpec, new LoggingProgressIndicator(outDir.toString()));
  }

  public static File mergeQueryableIndex(
      List<LuceneQueryableIndex> indexes,
      final AggregatorFactory[] metricAggs,
      File outDir,
      final IndexSpec indexSpec,
      ProgressIndicator progress
  ) throws IOException
  {
    FileUtils.deleteDirectory(outDir);
    if (!outDir.mkdirs()) {
      throw new ISE("Couldn't make outdir[%s].", outDir);
    }

    Directory dir = new NIOFSDirectory(Paths.get(outDir.toURI()));
    IndexWriterConfig iwc = new IndexWriterConfig(new SimpleAnalyzer());
    iwc.setUseCompoundFile(false);
    WhaledbCodec.setCodes(iwc);
    IndexWriter writer = new IndexWriter(dir, iwc);
    Directory[] dirs = new Directory[indexes.size()];
    LuceneQueryableIndex index;
    for (int i=0;i<indexes.size();i++) {
      index = indexes.get(i);
      if(null != index) {
        dirs[i] = indexes.get(i).getDirectory();
      }
    }
    writer.addIndexes(dirs);
    writer.close();
    return outDir;
  }
}
