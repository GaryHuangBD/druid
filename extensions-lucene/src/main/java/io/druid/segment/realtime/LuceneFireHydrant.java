/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.realtime;

import com.google.common.base.Throwables;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.LuceneIncrementalIndexSegment;
import io.druid.segment.ReferenceCountingSegment;
import io.druid.segment.Segment;
import io.druid.segment.incremental.IncrementalIndex;

import java.io.IOException;

/**
 */
public class LuceneFireHydrant
{
  private final int count;
  private volatile IncrementalIndex index;
  private volatile ReferenceCountingSegment adapter;

  public LuceneFireHydrant(
      IncrementalIndex index,
      int count,
      String segmentIdentifier
  )
  {
    this.index = index;
    this.adapter = new ReferenceCountingSegment(new LuceneIncrementalIndexSegment(index, segmentIdentifier));
    this.count = count;
  }

  public LuceneFireHydrant(
      Segment adapter,
      int count
  )
  {
    this.index = null;
    this.adapter = new ReferenceCountingSegment(adapter);
    this.count = count;
  }

  public IncrementalIndex getIndex()
  {
    return index;
  }

  public ReferenceCountingSegment getSegment()
  {
    return adapter;
  }

  public int getCount()
  {
    return count;
  }

  public boolean hasSwapped()
  {
    return index == null;
  }

  public void swapSegment(Segment adapter)
  {
    if (this.adapter != null) {
      try {
        this.adapter.close();
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    this.adapter = new ReferenceCountingSegment(adapter);
    this.index = null;
  }

  @Override
  public String toString()
  {
    return "FireHydrant{" +
           "index=" + index +
           ", queryable=" + adapter +
           ", count=" + count +
           '}';
  }
}
