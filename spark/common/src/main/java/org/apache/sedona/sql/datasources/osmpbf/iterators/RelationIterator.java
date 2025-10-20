/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.sedona.sql.datasources.osmpbf.iterators;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.apache.sedona.sql.datasources.osmpbf.build.Osmformat;
import org.apache.sedona.sql.datasources.osmpbf.features.TagsResolver;
import org.apache.sedona.sql.datasources.osmpbf.model.OSMEntity;
import org.apache.sedona.sql.datasources.osmpbf.model.Relation;
import org.apache.sedona.sql.datasources.osmpbf.model.RelationType;

public class RelationIterator implements Iterator<OSMEntity> {
  int idx;
  long relationCount;
  List<Osmformat.Relation> relations;
  Osmformat.StringTable stringTable;

  public RelationIterator(List<Osmformat.Relation> relations, Osmformat.StringTable stringTable) {
    this.idx = 0;
    this.relationCount = 0;
    this.relations = relations;
    this.stringTable = stringTable;

    if (relations != null) {
      this.relationCount = relations.size();
    }
  }

  @Override
  public boolean hasNext() {
    return idx < relationCount;
  }

  @Override
  public OSMEntity next() {
    if (idx < relationCount) {
      Relation relation = extract(idx);
      idx++;
      return relation;
    }

    return null;
  }

  public Relation extract(int idx) {
    Osmformat.Relation relation = relations.get(idx);

    return parse(relation);
  }

  private Relation parse(Osmformat.Relation relation) {
    if (relation == null) {
      return null;
    }

    long[] refs = resolveMemberIds(relation);
    String[] refTypes = resolveTypes(relation);
    String[] roles = resolveRefRoles(relation);

    HashMap<String, String> tags =
        TagsResolver.resolveTags(
            relation.getKeysCount(), relation::getKeys, relation::getVals, stringTable);

    return new Relation(relation.getId(), tags, refs, refTypes, roles);
  }

  private String[] resolveRefRoles(Osmformat.Relation relation) {
    String[] roles = new String[relation.getRolesSidCount()];

    for (int i = 0; i < relation.getRolesSidCount(); i++) {
      int role = relation.getRolesSid(i);
      roles[i] = stringTable.getS(role).toStringUtf8();
    }

    return roles;
  }

  private long[] resolveMemberIds(Osmformat.Relation relation) {
    long[] memberIds = new long[relation.getMemidsCount()];

    if (relation.getMemidsCount() != 0) {
      long idValue = relation.getMemids(0);

      memberIds[0] = idValue;

      for (int i = 1; i < relation.getMemidsCount(); i++) {
        idValue += relation.getMemids(i);
        memberIds[i] = idValue;
      }
    }

    return memberIds;
  }

  private String[] resolveTypes(Osmformat.Relation relation) {
    String[] types = new String[relation.getTypesCount()];

    for (int i = 0; i < relation.getTypesCount(); i++) {
      Osmformat.Relation.MemberType memberType = relation.getTypes(i);
      types[i] = RelationType.fromValue(memberType.getNumber());
    }

    return types;
  }
}
