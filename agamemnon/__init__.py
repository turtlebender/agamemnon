# Copyright 2010 University of Chicago
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from datetime import datetime
import logging
import pycassa.batch as batch
from pycassa.cassandra.ttypes import NotFoundException
import pycassa.system_manager as system_manager
import pycassa.columnfamily as cf
import agamemnon.primitives as prim
from graph_constants import *

log = logging.getLogger(__name__)

class NoTransactionError(Exception):
    pass


class RelationshipIndexEntry(object):
    def __init__(self, cf, key, rel_key):
        self._entry = {rel_key: {'__cf': cf, '__key': key, '__rel': rel_key}}

    @property
    def entry(self):
        return self._entry


class DataStore(object):
    def __init__(self, keyspace, pool, system_manager):
        self._cf_cache = {}
        self._index_cache = {}
        self._system_manager = system_manager
        self._pool = pool
        self._keyspace = keyspace
        self.outbound_rel_index_cf = 'outbound__%s' % RELATIONSHIP_INDEX
        self.inbound_rel_index_cf = 'inbound__%s' % RELATIONSHIP_INDEX
        if not self.cf_exists(self.outbound_rel_index_cf):
            self.create_cf(self.outbound_rel_index_cf, super=True)
        if not self.cf_exists(self.inbound_rel_index_cf):
            self.create_cf(self.inbound_rel_index_cf, super=True)

    def create_cf(self, type, column_type=system_manager.ASCII_TYPE, super=False, index_columns=list()):
        self._system_manager.create_column_family(self._keyspace, type, super=super, comparator_type=column_type)
        for column in index_columns:
            self._system_manager.create_index(self._keyspace, type, column, column_type,
                                              index_name='%s_%s_index' % (type, column))

    def cf_exists(self, type):
        try:
            cf.ColumnFamily(self._pool, type)
        except NotFoundException:
            return False
        return True

    def get_cf(self, type):
        if type in self._cf_cache.keys():
            return self._cf_cache[type]
        else:
            try:
                column_family = cf.ColumnFamily(self._pool, type)
                self._cf_cache[type] = column_family
            except NotFoundException:
                return None
            return column_family

    def get(self, type, row_key, super_column_key=None):
        column_family = self.get_cf(type)
        if column_family is None:
            self.create_cf(type)
            column_family = self.get_cf(type)
        if super_column_key is None:
            values = column_family.get(row_key)
        else:
            values = column_family.get(row_key, super_column=super_column_key)
        return values

    def delete(self, type, key, super_key=None):
        if super_key is None:
            self.get_cf(type).remove(key)
        else:
            self.get_cf(type).remove(key, super_key)

    def insert(self, type, key, args, super_key=None):
        if not self.cf_exists(type):
            self.create_cf(type)
        column_family = self.get_cf(type)
        if super_key is None:
            column_family.insert(key, args)
        else:
            column_family.insert(key, {super_key: args})

    def get_outgoing_relationships(self, source_node, rel_type):
        source_key = RELATIONSHIP_KEY_PATTERN % (source_node.type, source_node.key)
        cf = self.get_cf(OUTBOUND_RELATIONSHIP_CF)
        #Ok, this is weird.  So in order to get a column slice, you need to provide a start that is <= your first column
        #id, and a finish which is >= your last column.  Since our columns are sorted by ascii, this means we need to go
        #from rel_type_ to rel_type` because "`" is the char 1 greater than "_", so this will get anything which starts
        #rel_type_.  Now I realize that this could problems when there is a "_" in the relationship name, so we will
        #probably need a different delimiter.
        #TODO: fix delimiter
        try:
            super_columns = cf.get(source_key, column_start='%s__' % rel_type, column_finish='%s_`' % rel_type)
        except NotFoundException:
            super_columns = {}
        return [self.get_outgoing_relationship(rel_type, source_node, super_column) for super_column in
                super_columns.items()]


    def get_incoming_relationships(self, target_node, rel_type):
        target_key = RELATIONSHIP_KEY_PATTERN % (target_node.type, target_node.key)
        cf = self.get_cf(INBOUND_RELATIONSHIP_CF)
        #Ok, this is weird.  So in order to get a column slice, you need to provide a start that is <= your first column
        #id, and a finish which is >= your last column.  Since our columns are sorted by ascii, this means we need to go
        #from rel_type_ to rel_type` because "`" is the char 1 greater than "_", so this will get anything which starts
        #rel_type_.  Now I realize that this could problems when there is a "_" in the relationship name, so we will
        #probably need a different delimiter.
        #TODO: fix delimiter
        try:
            super_columns = cf.get(target_key, column_start='%s__' % rel_type, column_finish='%s_`' % rel_type)
        except NotFoundException:
            super_columns = {}
        return [self.get_incoming_relationship(rel_type, target_node, super_column) for super_column in
                super_columns.items()]

    def get_outgoing_relationship(self, rel_type, source_node, super_column):
        """
        Process the contents of a SuperColumn to extract the relationship and to_node properties and return
        a constructed relationship
        """
        rel_key = super_column[0]
        target_node_key = None
        target_node_type = None
        target_attributes = {}
        rel_attributes = {}
        for column in super_column[1].keys():
            value = super_column[1][column]
            if column == 'target__type':
                target_node_type = value
            elif column == 'target__key':
                target_node_key = value
            elif column.startswith('target__'):
                target_attributes[column[8:]] = value
            else:
                rel_attributes[column] = value
        return prim.Relationship(rel_key, source_node,
                                 prim.Node(self, target_node_type, target_node_key, target_attributes), self
                                 , rel_type,
                                 rel_attributes)

    def get_incoming_relationship(self, rel_type, target_node, super_column):
        """
        Process the contents of a SuperColumn to extract an incoming relationship and the associated from_node and
        return a constructed relationship
        """
        rel_key = super_column[0]
        source_node_key = None
        source_node_type = None
        source_attributes = {}
        rel_attributes = {}
        for column in super_column[1].keys():
            value = super_column[1][column]
            if column == 'source__type':
                source_node_type = value
            elif column == 'source__key':
                source_node_key = value
            elif column.startswith('source__'):
                source_attributes[column[8:]] = value
            else:
                rel_attributes[column] = value
        return prim.Relationship(rel_key, prim.Node(self, source_node_type, source_node_key, source_attributes),
                                 target_node,
                                 self, rel_type, rel_attributes)


    def delete_relationship(self, rel_type, rel_id, from_type, from_key, to_type, to_key):
        from_key = ENDPOINT_NAME_TEMPLATE % (from_type, from_key)
        to_key = ENDPOINT_NAME_TEMPLATE % (to_type, to_key)

        with batch.Mutator(self._pool) as b:
            b.remove(self.get_cf(INBOUND_RELATIONSHIP_CF), to_key,
                     super_column=RELATIONSHIP_KEY_PATTERN % (rel_type, rel_id))
            b.remove(self.get_cf(OUTBOUND_RELATIONSHIP_CF), from_key,
                     super_column=RELATIONSHIP_KEY_PATTERN % (rel_type, rel_id))

    def create_relationship(self, rel_type, source_node, target_node, key, args):
        if key is None:
            rel_key = RELATIONSHIP_KEY_PATTERN % (rel_type, datetime.now())
        else:
            rel_key = RELATIONSHIP_KEY_PATTERN % (rel_type, key)
            #node relationship types
        with batch.Mutator(self._pool) as b:
            #relationship attributes
            columns = {}
            columns.update(args)

            #outbound_cf
            columns = {'rel_type': rel_type, 'rel_key': key}
            #add relationship attributes
            columns.update(columns)
            #add target attributes
            columns['target__type'] = target_node.type.encode('ascii')
            columns['target__key'] = target_node.key.encode('ascii')
            target_attributes = target_node.attributes
            for attribute_key in target_attributes.keys():
                columns['target__%s' % attribute_key] = target_attributes[attribute_key]
            columns['source__type'] = source_node.type.encode('ascii')
            columns['source__key'] = source_node.key.encode('ascii')
            source_attributes = source_node.attributes
            for attribute_key in source_attributes.keys():
                columns['source__%s' % attribute_key] = source_attributes[attribute_key]

            source_key = ENDPOINT_NAME_TEMPLATE % (source_node.type, source_node.key)
            target_key = ENDPOINT_NAME_TEMPLATE % (target_node.type, target_node.key)
            inbound_rel_index_cf = self.get_cf(INBOUND_RELATIONSHIP_CF)
            outbound_rel_index_cf = self.get_cf(OUTBOUND_RELATIONSHIP_CF)

            b.insert(outbound_rel_index_cf, source_key, {rel_key: columns})
            b.insert(inbound_rel_index_cf, target_key, {rel_key: columns})

        #created relationship object
        return prim.Relationship(rel_key, source_node, target_node, self, rel_type, args)

    def create_node(self, type, key, args=None, reference=False):
        node = prim.Node(self, type, key, args)
        self.insert(type, key, args)
        if not reference:
            #this adds the created node to the reference node for this type of object
            #that reference node functions as an index to easily access all nodes of a specific type
            reference_node = self.get_reference_node(type)
            reference_node.instance(node, key=key)
        return node

    def delete_node(self, node):
        node_key = ENDPOINT_NAME_TEMPLATE % (node.type, node.key)
        try:
            outbound_results = self.get(OUTBOUND_RELATIONSHIP_CF, node_key)
        except NotFoundException:
            outbound_results = {}
        try:
            inbound_results = self.get(INBOUND_RELATIONSHIP_CF, node_key)
        except NotFoundException:
            inbound_results = {}

        with batch.Mutator(self._pool) as b:
            for rel in outbound_results:
                b.remove(self.get_cf(INBOUND_RELATIONSHIP_CF),
                         RELATIONSHIP_KEY_PATTERN % (rel['target__type'], rel['target__key']),
                         super_column='%s__%s' % (rel['rel_type'], rel['rel_key']))
            for rel in inbound_results:
                b.remove(self.get_cf(OUTBOUND_RELATIONSHIP_CF),
                         RELATIONSHIP_KEY_PATTERN % (rel['source__type'], rel['source__key']),
                         super_column='%s__%s' % (rel['rel_type'], rel['rel_key']))
            b.remove(self.get_cf(OUTBOUND_RELATIONSHIP_CF), node_key)
            b.remove(self.get_cf(INBOUND_RELATIONSHIP_CF), node_key)
            b.remove(self.get_cf(node.type), node.key)

    def save_node(self, node):
        """
        This needs to update the entry in the type table as well as all of the relationships
        """
        source_key = ENDPOINT_NAME_TEMPLATE % (node.type, node.key)
        target_key = ENDPOINT_NAME_TEMPLATE % (node.type, node.key)

        try:
            outbound_results = self.get(OUTBOUND_RELATIONSHIP_CF, source_key)
        except NotFoundException:
            outbound_results = {}
        try:
            inbound_results = self.get(INBOUND_RELATIONSHIP_CF, target_key)
        except NotFoundException:
            inbound_results = {}
        outbound_columns = {}
        outbound_columns['source__type'] =  node.type.encode('utf-8')
        outbound_columns['source__key'] = node.key.encode('utf-8')
        node_attributes = node.attributes
        for attribute_key in node.attributes.keys():
            outbound_columns['source__%s' % attribute_key] = node_attributes[attribute_key]
        for key in outbound_results.keys():
            target = outbound_results[key]
            target_key = ENDPOINT_NAME_TEMPLATE %(target['target__type'], target['target__key'])
            self.insert(OUTBOUND_RELATIONSHIP_CF, source_key, outbound_columns, key)
            self.insert(INBOUND_RELATIONSHIP_CF, target_key, outbound_columns, key)
        inbound_columns = {}
        inbound_columns['target__type'] =  node.type.encode('utf-8')
        inbound_columns['target__key'] = node.key.encode('utf-8')
        for attribute_key in node.attributes.keys():
            inbound_columns['target__%s' % attribute_key] = node_attributes[attribute_key]
        for key in inbound_results.keys():
            source = inbound_results[key]
            source_key = ENDPOINT_NAME_TEMPLATE %(source['source__type'], source['source__key'])
            self.insert(OUTBOUND_RELATIONSHIP_CF, source_key, inbound_columns, key)
            self.insert(INBOUND_RELATIONSHIP_CF, target_key, inbound_columns, key)
        self.insert(node.type, node.key, node.attributes)

    def get_node(self, type, key):
        try:
            values = self.get(type, key)
        except NotFoundException:
            raise NodeNotFoundException()
        return prim.Node(self, type, key, values)

    def get_reference_node(self, name):
        """
        Nodes returned here are very easily referenced by name and then function as an index for all attached nodes
        The most typical use case is to index all of the nodes of a certain type, but the functionality is not limited
        to this.  
        """
        try:
            node = self.get_node('reference', name)
        except NodeNotFoundException:
            node = self.create_node('reference', name, {'reference': 'reference'}, reference=True)
        return node


class NodeNotFoundException(Exception):
    pass


def _get_args(args, **kwargs):
    tmp_args = args
    if tmp_args is not None:
        if kwargs is not None:
            tmp_args.update(kwargs)
    else:
        if kwargs is not None:
            tmp_args = kwargs
    return tmp_args


def DFS(node, relationship_type, return_predicate=None):
    visited = set([node.key])
    S = [relationship for relationship in getattr(node, relationship_type)]
    while S:
        p = S.pop()
        relationship = p
        child = relationship.target_node
        if child.key not in visited:
            if return_predicate is not None and return_predicate(relationship, child):
                visited.add(child.key)
                yield child
            elif return_predicate is None:
                visited.add(child.key)
                yield child
            if hasattr(child, relationship_type):
                visited.add(child.key)
                S.extend([relationship for relationship in getattr(child, relationship_type)])
