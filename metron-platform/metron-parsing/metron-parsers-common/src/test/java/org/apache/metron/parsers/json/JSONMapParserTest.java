/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.metron.parsers.json;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.adrianwalker.multilinestring.Multiline;
import org.apache.log4j.Level;
import org.apache.metron.parsers.BasicParser;
import org.apache.metron.parsers.interfaces.MessageParser;
import org.apache.metron.test.utils.UnitTestHelper;
import org.json.simple.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JSONMapParserTest {

  private JSONMapParser parser;

  @Before
  public void setup() {
    parser = new JSONMapParser();
  }

  /**
   {
     "foo" : "bar"
    ,"blah" : "blah"
    ,"number" : 2.0
   }
   */
   @Multiline
   static String happyPathJSON;

  @Test
  public void testHappyPath() {
    List<JSONObject> output = parser.parse(happyPathJSON.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(output.size(), 1);
    //don't forget the timestamp field!
    Assert.assertEquals(output.get(0).size(), 4);
    JSONObject message = output.get(0);
    Assert.assertEquals("bar", message.get("foo"));
    Assert.assertEquals("blah", message.get("blah"));
    Assert.assertNotNull(message.get("timestamp"));
    Assert.assertTrue(message.get("timestamp") instanceof Number);
    Assert.assertNotNull(message.get("number"));
    Assert.assertTrue(message.get("number") instanceof Number);
  }

  /**
   {
    "collection" : { "blah" : 7, "blah2" : "foo", "bigblah" : { "innerBlah" : "baz", "reallyInnerBlah" : { "color" : "grey" }}}
   }
   */
   @Multiline
   static String collectionHandlingJSON;

  /**
    {
     "collection" : {
        "key" : "value"
      },
     "key" : "value"
    }
   */
  @Multiline
  static String mixCollectionHandlingJSON;

  @Test
  public void testCollectionHandlingDrop() {
    List<JSONObject> output = parser.parse(collectionHandlingJSON.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(output.size(), 1);
    //don't forget the timestamp field!
    Assert.assertEquals(output.get(0).size(), 1);
    JSONObject message = output.get(0);
    Assert.assertNotNull(message.get("timestamp"));
    Assert.assertTrue(message.get("timestamp") instanceof Number);
  }

  @Test(expected=IllegalStateException.class)
  public void testCollectionHandlingError() {
    parser.configure(ImmutableMap.of(JSONMapParser.MAP_STRATEGY_CONFIG, JSONMapParser.MapStrategy.ERROR.name()));
    UnitTestHelper.setLog4jLevel(BasicParser.class, Level.FATAL);
    parser.parse(collectionHandlingJSON.getBytes(StandardCharsets.UTF_8));
    UnitTestHelper.setLog4jLevel(BasicParser.class, Level.ERROR);
  }


  @Test
  public void testCollectionHandlingAllow() {
    parser.configure(ImmutableMap.of(JSONMapParser.MAP_STRATEGY_CONFIG, JSONMapParser.MapStrategy.ALLOW.name()));
    List<JSONObject> output = parser.parse(collectionHandlingJSON.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(output.size(), 1);
    //don't forget the timestamp field!
    Assert.assertEquals(output.get(0).size(), 2);
    JSONObject message = output.get(0);
    Assert.assertNotNull(message.get("timestamp"));
    Assert.assertTrue(message.get("timestamp") instanceof Number);
  }

  @Test
  public void testCollectionHandlingUnfold() {
    parser.configure(ImmutableMap.of(JSONMapParser.MAP_STRATEGY_CONFIG, JSONMapParser.MapStrategy.UNFOLD.name()));
    List<JSONObject> output = parser.parse(collectionHandlingJSON.getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals(output.size(), 1);
    //don't forget the timestamp field!
    Assert.assertEquals(output.get(0).size(), 5);
    JSONObject message = output.get(0);
    Assert.assertEquals(message.get("collection.blah"), 7);
    Assert.assertEquals(message.get("collection.blah2"), "foo");
    Assert.assertEquals(message.get("collection.bigblah.innerBlah"),"baz");
    Assert.assertEquals(message.get("collection.bigblah.reallyInnerBlah.color"),"grey");
    Assert.assertNotNull(message.get("timestamp"));
    Assert.assertTrue(message.get("timestamp") instanceof Number);
  }

  @Test
  public void testMixedCollectionHandlingUnfold() {
    parser.configure(ImmutableMap.of(JSONMapParser.MAP_STRATEGY_CONFIG,JSONMapParser.MapStrategy.UNFOLD.name()));
      List<JSONObject> output = parser.parse(mixCollectionHandlingJSON.getBytes(
              StandardCharsets.UTF_8));
    Assert.assertEquals(output.get(0).size(), 3);
    JSONObject message = output.get(0);
    Assert.assertEquals(message.get("collection.key"), "value");
    Assert.assertEquals(message.get("key"),"value");
    Assert.assertNotNull(message.get("timestamp"));
    Assert.assertTrue(message.get("timestamp") instanceof Number );
  }

  @Test
  public void getsReadCharsetFromConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put(MessageParser.READ_CHARSET, StandardCharsets.UTF_16.toString());
    parser.configure(config);
    assertThat(parser.getReadCharset(), equalTo(StandardCharsets.UTF_16));
  }

  @Test
  public void getsReadCharsetFromDefault() {
    Map<String, Object> config = new HashMap<>();
    parser.configure(config);
    assertThat(parser.getReadCharset(), equalTo(StandardCharsets.UTF_8));
  }
}
