/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import com.google.common.base.Function;
import com.google.common.collect.*;
import com.google.common.io.Files;
import com.metamx.common.guava.Sequences;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.select.*;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.data.Indexed;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.*;

@RunWith(Parameterized.class)
public class LuceneIndexMakerTest
{
  @Rule
  public final CloserRule closer = new CloserRule(false);
  private static final long TIMESTAMP = DateTime.parse("2014-01-01").getMillis();

  public static final String segmentId = "testSegment";

  private static final AggregatorFactory[] DEFAULT_AGG_FACTORIES = new AggregatorFactory[]{
      new CountAggregatorFactory(
          "count"
      )
  };

  static IndexSpec makeIndexSpec(
          BitmapSerdeFactory bitmapSerdeFactory,
          CompressedObjectStrategy.CompressionStrategy compressionStrategy,
          CompressedObjectStrategy.CompressionStrategy dimCompressionStrategy
  )
  {
    if (bitmapSerdeFactory != null || compressionStrategy != null) {
      return new IndexSpec(
              bitmapSerdeFactory,
              compressionStrategy.name().toLowerCase(),
              dimCompressionStrategy.name().toLowerCase()
      );
    } else {
      return new IndexSpec();
    }
  }

  private static final IndexSpec INDEX_SPEC = makeIndexSpec(
      new ConciseBitmapSerdeFactory(),
      CompressedObjectStrategy.CompressionStrategy.LZ4,
      CompressedObjectStrategy.CompressionStrategy.LZ4
  );
  private static final List<String> DIMS = ImmutableList.of("dim0", "dim1");

  private static final Function<Collection<Map<String, Object>>, Object[]> OBJECT_MAKER = new Function<Collection<Map<String, Object>>, Object[]>()
  {
    @Nullable
    @Override
    public Object[] apply(Collection<Map<String, Object>> input)
    {
      final ArrayList<InputRow> list = new ArrayList<>();
      int i = 0;
      for (final Map<String, Object> map : input) {
        list.add(new MapBasedInputRow(TIMESTAMP + i++, DIMS, map));
      }
      return new Object[]{list};
    }
  };

  @SafeVarargs
  public static Collection<Object[]> permute(Map<String, Object>... maps)
  {
    if (maps == null) {
      return ImmutableList.<Object[]>of();
    }
    return Collections2.transform(
        Collections2.permutations(
            Arrays.asList(maps)
        ),
        OBJECT_MAKER
    );
  }

  @Parameterized.Parameters
  public static Iterable<Object[]> paramFeeder()
  {
    final Map<String, Object> map1 = ImmutableMap.<String, Object>of(
        //DIMS.get(0), ImmutableList.<String>of("dim00", "dim01"),
        DIMS.get(0), "dim00",
        DIMS.get(1), "dim10"
    );

    final List<String> nullList = Collections.singletonList(null);

    final Map<String, Object> map2 = ImmutableMap.<String, Object>of(
        DIMS.get(0), "dim01",
        DIMS.get(1), "dim11"
    );
    final Map<String, Object> map3 = ImmutableMap.<String, Object>of(
            DIMS.get(0), "dim01",
            DIMS.get(1), "dim12"
    );


//    final Map<String, Object> map3 = ImmutableMap.<String, Object>of(
//        DIMS.get(0),
//        ImmutableList.<String>of("dim00", "dim01")
//    );
//
//    final Map<String, Object> map4 = ImmutableMap.<String, Object>of();
//
//    final Map<String, Object> map5 = ImmutableMap.<String, Object>of(DIMS.get(1), "dim10");
//
//    final Map<String, Object> map6 = new HashMap<>();
//    map6.put(DIMS.get(1), null); // ImmutableMap cannot take null


    return Iterables.<Object[]>concat(
        permute(map1,map2,map3)
//        , permute(map1, map4)
//        , permute(map1, map5)
//        , permute(map5, map6)
//        , permute(map4, map5)
//        , Iterables.transform(ImmutableList.of(Arrays.asList(map1, map2, map3, map4, map5, map6)), OBJECT_MAKER)
    );

  }

  private final Collection<InputRow> events;

  public LuceneIndexMakerTest(
      final Collection<InputRow> events
  )
  {
    this.events = events;
  }

  IncrementalIndex toPersist;
  File outDir;
//  File persistTmpDir;

  @Before
  public void setUp() throws IOException
  {
    outDir = Files.createTempDir();
    toPersist = new OnheapIncrementalIndex(
        JodaUtils.MIN_INSTANT,
        QueryGranularity.NONE,
        DEFAULT_AGG_FACTORIES,
        1000000
    );
    for (InputRow event : events) {
      toPersist.add(event);
    }
//    tmpDir = Files.createTempDir();
//    persistTmpDir = new File(tmpDir, "persistDir");
//    IndexMaker.persist(toPersist, persistTmpDir, null, INDEX_SPEC);
  }

  @After
  public void tearDown() throws IOException
  {
    if (outDir != null) {
      FileUtils.deleteDirectory(outDir);
    }
  }

  @Test
  public void testPersistWithSegmentMetadata() throws IOException
  {
    QueryableIndex index = null;
    try {
      Map<String, Object> segmentMetadata = ImmutableMap.<String, Object>of("key", "value");
      LuceneIndexMaker.persist(toPersist, outDir, segmentMetadata, INDEX_SPEC);
      index = LuceneIndexIO.loadIndex(outDir);
      Indexed<String> cols = index.getColumnNames();
      DictionaryEncodedColumn dictionaryEncodedColumn;
      for (String col : cols) {
        dictionaryEncodedColumn = index.getColumn(col).getDictionaryEncoding();
        String id = dictionaryEncodedColumn.lookupName(0);
        System.out.println(id);
      }
    }
    finally {
      if (index != null) {
        index.close();
      }

    }
  }



  @Test
  public void testQUery() throws IOException {

    Map<String, Object> segmentMetadata = ImmutableMap.<String, Object>of("key", "value");
    LuceneIndexMaker.persist(toPersist, outDir, segmentMetadata, INDEX_SPEC);

    try {
      SelectQuery query = new SelectQuery(
              new TableDataSource(""),
              QueryRunnerTestHelper.fullOnInterval,
              new SelectorDimFilter(DIMS.get(0), "dim01"),
//              null,
              QueryRunnerTestHelper.allGran,
              Lists.<String>newArrayList(),
              Lists.<String>newArrayList(),
              new PagingSpec(null, 3),
              null
      );

      QueryableIndex queryableIndex = LuceneIndexIO.loadIndex(outDir);
      QueryableIndexSegment segment = new QueryableIndexSegment(LuceneIndexMakerTest.segmentId, queryableIndex);
      SelectQueryRunnerFactory factory = new SelectQueryRunnerFactory(
              new SelectQueryQueryToolChest(
                      new DefaultObjectMapper(),
                      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
              ),
              new SelectQueryEngine(),
              QueryRunnerTestHelper.NOOP_QUERYWATCHER
      );
      QueryRunner<Result<SelectResultValue>> runner = QueryRunnerTestHelper.makeQueryRunner(factory, segment);


      HashMap<String,Object> context = new HashMap<String, Object>();
      Iterable<Result<SelectResultValue>> results = Sequences.toList(
              runner.run(query, context),
              Lists.<Result<SelectResultValue>>newArrayList()
      );

      System.out.println(results.toString());


    } finally {

    }
  }


}
