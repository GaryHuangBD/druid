package io.druid.query;

import com.google.common.base.Function;
import com.google.common.collect.*;
import com.google.common.io.Files;
import com.metamx.common.guava.Sequences;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.select.*;
import io.druid.segment.*;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.data.LuceneBitmapSerdeFactory;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by garyhuang on 2016/4/26.
 */
//@RunWith(Parameterized.class)
public class LuceneQueryableIndexTest {
//    @Parameterized.Parameters
//    public static Iterable<Object[]> constructorFeeder() throws IOException
//    {
//        return QueryRunnerTestHelper.transformToConstructionFeeder(
//                QueryRunnerTestHelper.makeQueryRunners(
//                        new SelectQueryRunnerFactory(
//                                new SelectQueryQueryToolChest(
//                                        new DefaultObjectMapper(),
//                                        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
//                                ),
//                                new SelectQueryEngine(),
//                                QueryRunnerTestHelper.NOOP_QUERYWATCHER
//                        )
//                )
//        );
//    }

//    private final QueryRunner runner;
//
//    public LuceneQueryableIndexTest(QueryRunner runner) {
//        this.runner = runner;
//    }


    File indexDir = null;
    @Before
    public void setUp() throws Exception {
        String filePath="C:\\Users\\garyhuang\\AppData\\Local\\Temp\\1461815709772-0";
        indexDir = new File(filePath);
    }

    @Test
    public void testQUery() throws IOException {


        try {
            SelectQuery query = new SelectQuery(
                    new TableDataSource(""),
                    QueryRunnerTestHelper.fullOnInterval,
                    null,
                    QueryRunnerTestHelper.allGran,
                    Lists.<String>newArrayList(),
                    Lists.<String>newArrayList(),
                    new PagingSpec(null, 3),
                    null
            );

            QueryableIndex queryableIndex = LuceneIndexIO.loadIndex(indexDir);
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

    @Test
    public void getDataInterval() throws Exception {

    }

    @Test
    public void getNumRows() throws Exception {

    }

    @Test
    public void getColumnNames() throws Exception {

    }

    @Test
    public void getAvailableDimensions() throws Exception {

    }

    @Test
    public void getBitmapFactoryForDimensions() throws Exception {

    }

    @Test
    public void getColumn() throws Exception {

    }

    @Test
    public void close() throws Exception {

    }

    @Test
    public void getMetaData() throws Exception {

    }

}