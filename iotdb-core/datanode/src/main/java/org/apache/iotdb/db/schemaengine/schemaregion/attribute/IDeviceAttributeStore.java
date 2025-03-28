/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.schemaengine.schemaregion.attribute;

import org.apache.tsfile.utils.Binary;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The storage of table device attributes. Note that we pass in the table name only for the metrics
 * and that does not appear in the real storage.
 */
public interface IDeviceAttributeStore {

  void clear();

  boolean createSnapshot(final File targetDir);

  void loadFromSnapshot(final File snapshotDir) throws IOException;

  int createAttribute(
      final List<String> nameList, final Object[] valueList, final String tableName);

  // Returns the actually updated map
  Map<String, Binary> alterAttribute(
      final int pointer,
      final List<String> nameList,
      final Object[] valueList,
      final String tableName);

  void removeAttribute(final int pointer, final String tableName);

  void removeAttribute(final int pointer, final String attributeName, final String tableName);

  Map<String, Binary> getAttributes(final int pointer);

  Binary getAttributes(final int pointer, final String name);
}
