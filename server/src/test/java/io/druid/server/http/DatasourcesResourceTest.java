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

package io.druid.server.http;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.druid.client.CoordinatorServerView;
import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.InventoryView;
import io.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class DatasourcesResourceTest
{
  private CoordinatorServerView inventoryView;
  private DruidServer server;
  private List<DruidDataSource> listDataSources;
  private List<DataSegment> dataSegmentList;

  @Before
  public void setUp()
  {
    inventoryView = EasyMock.createStrictMock(CoordinatorServerView.class);
    server = EasyMock.createStrictMock(DruidServer.class);
    dataSegmentList = new ArrayList<>();
    dataSegmentList.add(
        new DataSegment(
            "datasource1",
            new Interval("2010-01-01/P1D"),
            null,
            null,
            null,
            null,
            null,
            0x9,
            0
        )
    );
    dataSegmentList.add(
        new DataSegment(
            "datasource1",
            new Interval("2010-01-22/P1D"),
            null,
            null,
            null,
            null,
            null,
            0x9,
            0
        )
    );
    dataSegmentList.add(
        new DataSegment(
            "datasource2",
            new Interval("2010-01-01/P1D"),
            null,
            null,
            null,
            null,
            null,
            0x9,
            0
        )
    );
    listDataSources = new ArrayList<>();
    listDataSources.add(new DruidDataSource("datasource1", new HashMap()).addSegment("part1", dataSegmentList.get(0)));
    listDataSources.add(new DruidDataSource("datasource2", new HashMap()).addSegment("part1", dataSegmentList.get(1)));
  }

  @Test
  public void testGetFullQueryableDataSources() throws Exception
  {
    EasyMock.expect(server.getDataSources()).andReturn(
        ImmutableList.of(listDataSources.get(0), listDataSources.get(1))
    ).atLeastOnce();
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();
    EasyMock.replay(inventoryView, server);
    DatasourcesResource datasourcesResource = new DatasourcesResource(inventoryView, null, null);
    Response response = datasourcesResource.getQueryableDataSources("full", null);
    Set<DruidDataSource> result = (Set<DruidDataSource>) response.getEntity();
    DruidDataSource[] resultantDruidDataSources = new DruidDataSource[result.size()];
    result.toArray(resultantDruidDataSources);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(2, resultantDruidDataSources.length);
    Assert.assertArrayEquals(listDataSources.toArray(), resultantDruidDataSources);

    response = datasourcesResource.getQueryableDataSources(null, null);
    List<String> result1 = (List<String>) response.getEntity();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(2, result1.size());
    Assert.assertTrue(result1.contains("datasource1"));
    Assert.assertTrue(result1.contains("datasource2"));
    EasyMock.verify(inventoryView, server);
  }

  @Test
  public void testGetSimpleQueryableDataSources() throws Exception
  {
    EasyMock.expect(server.getDataSources()).andReturn(
        listDataSources
    ).atLeastOnce();
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(
        listDataSources.get(0)
    ).atLeastOnce();
    EasyMock.expect(server.getTier()).andReturn(null).atLeastOnce();
    EasyMock.expect(server.getDataSource("datasource2")).andReturn(
        listDataSources.get(1)
    ).atLeastOnce();
    EasyMock.expect(server.getTier()).andReturn(null).atLeastOnce();
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();

    EasyMock.replay(inventoryView, server);
    DatasourcesResource datasourcesResource = new DatasourcesResource(inventoryView, null, null);
    Response response = datasourcesResource.getQueryableDataSources(null, "simple");
    Assert.assertEquals(200, response.getStatus());
    List<Map<String, Object>> results = (List<Map<String, Object>>) response.getEntity();
    int index = 0;
    for (Map<String, Object> entry : results) {
      Assert.assertEquals(listDataSources.get(index).getName(), entry.get("name").toString());
      Assert.assertTrue(((Map) ((Map) entry.get("properties")).get("tiers")).containsKey(null));
      Assert.assertNotNull((((Map) entry.get("properties")).get("segments")));
      Assert.assertEquals(1, ((Map) ((Map) entry.get("properties")).get("segments")).get("count"));
      index++;
    }
    EasyMock.verify(inventoryView, server);
  }

  @Test
  public void testFullGetTheDataSource() throws Exception
  {
    DruidDataSource dataSource1 = new DruidDataSource("datasource1", new HashMap());
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(
        dataSource1
    ).atLeastOnce();
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();

    EasyMock.replay(inventoryView, server);
    DatasourcesResource datasourcesResource = new DatasourcesResource(inventoryView, null, null);
    Response response = datasourcesResource.getTheDataSource("datasource1", "full");
    DruidDataSource result = (DruidDataSource) response.getEntity();
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(dataSource1, result);
    EasyMock.verify(inventoryView, server);
  }

  @Test
  public void testNullGetTheDataSource() throws Exception
  {
    EasyMock.expect(server.getDataSource("none")).andReturn(null).atLeastOnce();
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();

    EasyMock.replay(inventoryView, server);
    DatasourcesResource datasourcesResource = new DatasourcesResource(inventoryView, null, null);
    Assert.assertEquals(204, datasourcesResource.getTheDataSource("none", null).getStatus());
    EasyMock.verify(inventoryView, server);
  }

  @Test
  public void testSimpleGetTheDataSource() throws Exception
  {
    DruidDataSource dataSource1 = new DruidDataSource("datasource1", new HashMap());
    dataSource1.addSegment(
        "partition",
        new DataSegment("datasegment1", new Interval("2010-01-01/P1D"), null, null, null, null, null, 0x9, 0)
    );
    EasyMock.expect(server.getDataSource("datasource1")).andReturn(
        dataSource1
    ).atLeastOnce();
    EasyMock.expect(server.getTier()).andReturn(null).atLeastOnce();
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();

    EasyMock.replay(inventoryView, server);
    DatasourcesResource datasourcesResource = new DatasourcesResource(inventoryView, null, null);
    Response response = datasourcesResource.getTheDataSource("datasource1", null);
    Assert.assertEquals(200, response.getStatus());
    Map<String, Map<String, Object>> result = (Map<String, Map<String, Object>>) response.getEntity();
    Assert.assertEquals(1, ((Map) (result.get("tiers").get(null))).get("segmentCount"));
    Assert.assertNotNull(result.get("segments"));
    Assert.assertNotNull(result.get("segments").get("minTime").toString(), "2010-01-01T00:00:00.000Z");
    Assert.assertNotNull(result.get("segments").get("maxTime").toString(), "2010-01-02T00:00:00.000Z");
    EasyMock.verify(inventoryView, server);
  }

  @Test
  public void testGetSegmentDataSourceIntervals()
  {
    server = new DruidServer("who", "host", 1234, "historical", "tier1", 0);
    server.addDataSegment(dataSegmentList.get(0).getIdentifier(), dataSegmentList.get(0));
    server.addDataSegment(dataSegmentList.get(1).getIdentifier(), dataSegmentList.get(1));
    server.addDataSegment(dataSegmentList.get(2).getIdentifier(), dataSegmentList.get(2));
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();
    EasyMock.replay(inventoryView);

    List<Interval> expectedIntervals = new ArrayList<>();
    expectedIntervals.add(new Interval("2010-01-22T00:00:00.000Z/2010-01-23T00:00:00.000Z"));
    expectedIntervals.add(new Interval("2010-01-01T00:00:00.000Z/2010-01-02T00:00:00.000Z"));
    DatasourcesResource datasourcesResource = new DatasourcesResource(inventoryView, null, null);

    Response response = datasourcesResource.getSegmentDataSourceIntervals("invalidDataSource", null, null);
    Assert.assertEquals(response.getEntity(), null);

    response = datasourcesResource.getSegmentDataSourceIntervals("datasource1", null, null);
    TreeSet<Interval> actualIntervals = (TreeSet) response.getEntity();
    Assert.assertEquals(2, actualIntervals.size());
    Assert.assertEquals(expectedIntervals.get(0), actualIntervals.first());
    Assert.assertEquals(expectedIntervals.get(1), actualIntervals.last());

    response = datasourcesResource.getSegmentDataSourceIntervals("datasource1", "simple", null);
    TreeMap<Interval, Map<String, Object>> results = (TreeMap) response.getEntity();
    Assert.assertEquals(2, results.size());
    Assert.assertEquals(expectedIntervals.get(0), results.firstKey());
    Assert.assertEquals(expectedIntervals.get(1), results.lastKey());
    Assert.assertEquals(1, results.firstEntry().getValue().get("count"));
    Assert.assertEquals(1, results.lastEntry().getValue().get("count"));

    response = datasourcesResource.getSegmentDataSourceIntervals("datasource1", null, "full");
    results = ((TreeMap<Interval, Map<String, Object>>) response.getEntity());
    int i = 1;
    for (Map.Entry<Interval, Map<String, Object>> entry : results.entrySet()) {
      Assert.assertEquals(dataSegmentList.get(i).getInterval(), entry.getKey());
      Assert.assertEquals(
          dataSegmentList.get(i),
          ((Map<String, Object>) entry.getValue().get(dataSegmentList.get(i).getIdentifier())).get(
              "metadata"
          )
      );
      i--;
    }
    EasyMock.verify(inventoryView);
  }

  @Test
  public void testGetSegmentDataSourceSpecificInterval()
  {
    server = new DruidServer("who", "host", 1234, "historical", "tier1", 0);
    server.addDataSegment(dataSegmentList.get(0).getIdentifier(), dataSegmentList.get(0));
    server.addDataSegment(dataSegmentList.get(1).getIdentifier(), dataSegmentList.get(1));
    server.addDataSegment(dataSegmentList.get(2).getIdentifier(), dataSegmentList.get(2));
    EasyMock.expect(inventoryView.getInventory()).andReturn(
        ImmutableList.of(server)
    ).atLeastOnce();
    EasyMock.replay(inventoryView);

    DatasourcesResource datasourcesResource = new DatasourcesResource(inventoryView, null, null);
    Response response = datasourcesResource.getSegmentDataSourceSpecificInterval(
        "invalidDataSource",
        "2010-01-01/P1D",
        null,
        null
    );
    Assert.assertEquals(null, response.getEntity());

    response = datasourcesResource.getSegmentDataSourceSpecificInterval(
        "datasource1",
        "2010-03-01/P1D",
        null,
        null
    ); // interval not present in the datasource
    Assert.assertEquals(ImmutableSet.of(), response.getEntity());

    response = datasourcesResource.getSegmentDataSourceSpecificInterval("datasource1", "2010-01-01/P1D", null, null);
    Assert.assertEquals(ImmutableSet.of(dataSegmentList.get(0).getIdentifier()), response.getEntity());

    response = datasourcesResource.getSegmentDataSourceSpecificInterval("datasource1", "2010-01-01/P1M", null, null);
    Assert.assertEquals(
        ImmutableSet.of(dataSegmentList.get(1).getIdentifier(), dataSegmentList.get(0).getIdentifier()),
        response.getEntity()
    );

    response = datasourcesResource.getSegmentDataSourceSpecificInterval(
        "datasource1",
        "2010-01-01/P1M",
        "simple",
        null
    );
    HashMap<Interval, Map<String, Object>> results = ((HashMap<Interval, Map<String, Object>>) response.getEntity());
    Assert.assertEquals(2, results.size());
    int i;
    for (i = 0; i < 2; i++) {
      Assert.assertTrue(results.containsKey(dataSegmentList.get(i).getInterval()));
      Assert.assertEquals(1, (results.get(dataSegmentList.get(i).getInterval())).get("count"));
    }

    response = datasourcesResource.getSegmentDataSourceSpecificInterval("datasource1", "2010-01-01/P1M", null, "full");
    TreeMap<Interval, Map<String, Object>> results1 = ((TreeMap<Interval, Map<String, Object>>) response.getEntity());
    i = 1;
    for (Map.Entry<Interval, Map<String, Object>> entry : results1.entrySet()) {
      Assert.assertEquals(dataSegmentList.get(i).getInterval(), entry.getKey());
      Assert.assertEquals(
          dataSegmentList.get(i),
          ((Map<String, Object>) entry.getValue().get(dataSegmentList.get(i).getIdentifier())).get(
              "metadata"
          )
      );
      i--;
    }
    EasyMock.verify(inventoryView);
  }
}
