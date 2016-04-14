package io.druid.segment;

import com.google.common.base.Preconditions;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.common.io.smoosh.SmooshedFileMapper;
import io.druid.segment.column.Column;
import io.druid.segment.data.Indexed;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Map;

/**
 * Created by garyhuang on 2016/4/13.
 */
public class LuceneQueryableIndex implements QueryableIndex {
    private final Interval dataInterval;
    private final Indexed<String> columnNames;
    private final Indexed<String> availableDimensions;
    private final BitmapFactory bitmapFactory;
    private final Map<String, Column> columns;
    private final Map<String, Object> metadata;

    public LuceneQueryableIndex(
            Interval dataInterval,
            Indexed<String> columnNames,
            Indexed<String> dimNames,
            BitmapFactory bitmapFactory,
            Map<String, Column> columns,
            Map<String, Object> metadata
    )
    {
        Preconditions.checkNotNull(columns.get(Column.TIME_COLUMN_NAME));
        this.dataInterval = dataInterval;
        this.columnNames = columnNames;
        this.availableDimensions = dimNames;
        this.bitmapFactory = bitmapFactory;
        this.columns = columns;
        this.metadata = metadata;
    }

    @Override
    public Interval getDataInterval()
    {
        return dataInterval;
    }

    @Override
    public int getNumRows()
    {
        return columns.get(Column.TIME_COLUMN_NAME).getLength();
    }

    @Override
    public Indexed<String> getColumnNames()
    {
        return columnNames;
    }

    @Override
    public Indexed<String> getAvailableDimensions()
    {
        return availableDimensions;
    }

    @Override
    public BitmapFactory getBitmapFactoryForDimensions()
    {
        return bitmapFactory;
    }

    @Override
    public Column getColumn(String columnName)
    {
        return columns.get(columnName);
    }

    @Override
    public void close() throws IOException
    {

    }

    @Override
    public Map<String, Object> getMetaData()
    {
        return metadata;
    }
}
