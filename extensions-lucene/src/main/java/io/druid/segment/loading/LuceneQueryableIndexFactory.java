package io.druid.segment.loading;

import com.metamx.common.logger.Logger;
import io.druid.segment.IndexIO;
import io.druid.segment.LuceneIndexIO;
import io.druid.segment.QueryableIndex;

import java.io.File;
import java.io.IOException;

/**
 * Created by garyhuang on 2016/4/13.
 */
public class LuceneQueryableIndexFactory implements QueryableIndexFactory
{
    private static final Logger log = new Logger(MMappedQueryableIndexFactory.class);

    @Override
    public QueryableIndex factorize(File parentDir) throws SegmentLoadingException
    {
        try {
            return LuceneIndexIO.loadIndex(parentDir);
        }
        catch (IOException e) {
            throw new SegmentLoadingException(e, "%s", e.getMessage());
        }
    }
}