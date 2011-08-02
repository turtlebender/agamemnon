from contextlib import contextmanager
import json
import string
import uuid
from pycassa.cassandra.ttypes import NotFoundException
from agamemnon.graph_constants import RELATIONSHIP_KEY_PATTERN, OUTBOUND_RELATIONSHIP_CF, RELATIONSHIP_INDEX, ENDPOINT_NAME_TEMPLATE, INBOUND_RELATIONSHIP_CF
import pycassa
from agamemnon.cassandra import CassandraDataStore
from agamemnon.memory import InMemoryDataStore
from agamemnon.exceptions import NodeNotFoundException
import agamemnon.primitives as prim


class DataStore(object):
    def __init__(self, delegate):
        self.delegate = delegate

    @contextmanager
    def batch(self):
        self.delegate.start_batch()
        yield
        self.delegate.commit_batch()

    def get(self, type, row_key, super_column_key=None):
        column_family = self.delegate.get_cf(type)
        if column_family is None:
            self.delegate.create_cf(type)
            column_family = self.delegate.get_cf(type)
        if super_column_key is None:
            return self.deserialize_value(column_family.get(row_key))
        else:
            return self.deserialize_value(column_family.get(row_key, super_column=super_column_key))

    def delete(self, type, key, super_key=None):
        if super_key is None:
            self.delegate.get_cf(type).remove(key)
        else:
            self.delegate.get_cf(type).remove(key, super_key)

    def insert(self, type, key, args, super_key=None):
        if not self.delegate.cf_exists(type):
            column_family = self.delegate.create_cf(type)
        else:
            column_family = self.delegate.get_cf(type)
        serialized = self.serialize_columns(args)
        if super_key is None:
            column_family.insert(key, serialized)
        else:
            column_family.insert(key, {super_key: serialized})

    def get_outgoing_relationships(self, source_node, rel_type, count=100):
        source_key = RELATIONSHIP_KEY_PATTERN % (source_node.type, source_node.key)
        cf = self.delegate.get_cf(OUTBOUND_RELATIONSHIP_CF)
        #Ok, this is weird.  So in order to get a column slice, you need to provide a start that is <= your first column
        #id, and a finish which is >= your last column.  Since our columns are sorted by ascii, this means we need to go
        #from rel_type_ to rel_type` because "`" is the char 1 greater than "_", so this will get anything which starts
        #rel_type_.  Now I realize that this could problems when there is a "_" in the relationship name, so we will
        #probably need a different delimiter.
        #TODO: fix delimiter
        try:
            super_columns = cf.get(source_key, column_start='%s__' % rel_type, column_finish='%s_`' % rel_type,
                                   column_count=count)
        except NotFoundException:
            super_columns = {}
        return [self.get_outgoing_relationship(rel_type, source_node, super_column) for super_column in
                super_columns.items()]


    def get_incoming_relationships(self, target_node, rel_type, count=100):
        target_key = RELATIONSHIP_KEY_PATTERN % (target_node.type, target_node.key)
        cf = self.delegate.get_cf(INBOUND_RELATIONSHIP_CF)
        #Ok, this is weird.  So in order to get a column slice, you need to provide a start that is <= your first column
        #id, and a finish which is >= your last column.  Since our columns are sorted by ascii, this means we need to go
        #from rel_type_ to rel_type` because "`" is the char 1 greater than "_", so this will get anything which starts
        #rel_type_.  Now I realize that this could problems when there is a "_" in the relationship name, so we will
        #probably need a different delimiter.
        #TODO: fix delimiter
        try:
            super_columns = cf.get(target_key, column_start='%s__' % rel_type, column_finish='%s_`' % rel_type,
                                   column_count=count)
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

        with self.batch():
            self.delegate.remove(self.delegate.get_cf(INBOUND_RELATIONSHIP_CF), to_key,
                                 super_column=RELATIONSHIP_KEY_PATTERN % (rel_type, rel_id))
            self.delegate.remove(self.delegate.get_cf(OUTBOUND_RELATIONSHIP_CF), from_key,
                                 super_column=RELATIONSHIP_KEY_PATTERN % (rel_type, rel_id))

    def create_relationship(self, rel_type, source_node, target_node, key=None, args=dict()):
        if key is None:
            key = str(uuid.uuid4())
            #node relationship types
        rel_key = RELATIONSHIP_KEY_PATTERN % (rel_type, key)
        with self.batch():
            #outbound_cf
            columns = {'rel_type': rel_type, 'rel_key': key}
            #add relationship attributes
            columns.update(args)
            rel_attr = dict(columns)
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
            inbound_rel_index_cf = self.delegate.get_cf(INBOUND_RELATIONSHIP_CF)
            outbound_rel_index_cf = self.delegate.get_cf(OUTBOUND_RELATIONSHIP_CF)
            serialized = self.serialize_columns(columns)
            self.delegate.insert(outbound_rel_index_cf, source_key, {rel_key: serialized})
            self.delegate.insert(inbound_rel_index_cf, target_key, {rel_key: serialized})

            relationship_index_cf = self.delegate.get_cf(RELATIONSHIP_INDEX)
            # Add entries in the relationship index
            self.delegate.insert(relationship_index_cf, source_key,
                    {target_node.key: {rel_type: '%s__outgoing' % rel_key}})
            self.delegate.insert(relationship_index_cf, target_key,
                    {source_node.key: {rel_type: '%s__incoming' % rel_key}})

        #created relationship object
        return prim.Relationship(rel_key, source_node, target_node, self, rel_type, rel_attr)

    def has_relationship(self, node_a, node_b_key, rel_type):
        """
        This determines if two nodes have a relationship of the specified type.

        > ds = DataStore()
        > node_a =

        """
        index = self.delegate.get_cf(RELATIONSHIP_INDEX)
        node_a_row_key = ENDPOINT_NAME_TEMPLATE % (node_a.type, node_a.key)
        rel_list = []
        try:
            rels = index.get(node_a_row_key, super_column=node_b_key, column_start=rel_type,
                             column_finish='%s`' % rel_type)
            for rel in rels.values():
                if rel.endswith('__incoming'):
                    rel_id = string.replace(rel, '__incoming', '')
                    relationship = self.get_incoming_relationship(rel_type, node_a,
                        (rel_id, self.get(INBOUND_RELATIONSHIP_CF, node_a_row_key, rel_id)))
                elif rel.endswith('__outgoing'):
                    rel_id = string.replace(rel, '__outgoing', '')
                    relationship = self.get_outgoing_relationship(rel_type, node_a,
                        (rel_id, self.get(OUTBOUND_RELATIONSHIP_CF, node_a_row_key, rel_id)))
                else:
                    continue
                rel_list.append(relationship)
        except NotFoundException:
            pass
        return rel_list

    def create_node(self, type, key, args=None, reference=False):
        node = prim.Node(self, type, key, args)
        if args is not None:
            serialized = self.serialize_columns(args)
        else:
            serialized = {}
        self.insert(type, key, serialized)
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

        with self.batch():
            for key in outbound_results.keys():
                rel = outbound_results[key]
                self.delegate.remove(self.delegate.get_cf(INBOUND_RELATIONSHIP_CF),
                                     RELATIONSHIP_KEY_PATTERN % (rel['target__type'], rel['target__key']),
                                     super_column='%s__%s' % (rel['rel_type'], rel['rel_key']))
            for key in inbound_results.keys():
                rel = inbound_results[key]
                self.delegate.remove(self.delegate.get_cf(OUTBOUND_RELATIONSHIP_CF),
                                     RELATIONSHIP_KEY_PATTERN % (rel['source__type'], rel['source__key']),
                                     super_column='%s__%s' % (rel['rel_type'], rel['rel_key']))
            self.delegate.remove(self.delegate.get_cf(OUTBOUND_RELATIONSHIP_CF), node_key)
            self.delegate.remove(self.delegate.get_cf(INBOUND_RELATIONSHIP_CF), node_key)
            self.delegate.remove(self.delegate.get_cf(node.type), node.key)

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
        outbound_columns = {'source__type': node.type.encode('utf-8'), 'source__key': node.key.encode('utf-8')}
        node_attributes = node.attributes
        for attribute_key in node.attributes.keys():
            outbound_columns['source__%s' % attribute_key] = node_attributes[attribute_key]
        for key in outbound_results.keys():
            target = outbound_results[key]
            target_key = ENDPOINT_NAME_TEMPLATE % (target['target__type'], target['target__key'])
            target.update(outbound_columns)
            serialized = self.serialize_columns(target)
            self.insert(OUTBOUND_RELATIONSHIP_CF, source_key, serialized, key)
            self.insert(INBOUND_RELATIONSHIP_CF, target_key, serialized, key)
        inbound_columns = {'target__type': node.type.encode('utf-8'), 'target__key': node.key.encode('utf-8')}
        for attribute_key in node.attributes.keys():
            inbound_columns['target__%s' % attribute_key] = node_attributes[attribute_key]
        for key in inbound_results.keys():
            source = inbound_results[key]
            source_key = ENDPOINT_NAME_TEMPLATE % (source['source__type'], source['source__key'])
            target_key = ENDPOINT_NAME_TEMPLATE % (node.type, node.key)
            source.update(inbound_columns)
            serialized = self.serialize_columns(source)
            self.insert(OUTBOUND_RELATIONSHIP_CF, source_key, serialized, key)
            self.insert(INBOUND_RELATIONSHIP_CF, target_key, serialized, key)
        self.insert(node.type, node.key, self.serialize_columns(node.attributes))

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

    def deserialize_value(self, value):
        if isinstance(value, dict):
            return self.deserialize_columns(value)
        if not value.startswith('$'):
            return value
        type = value[:2]
        content = value[2:]
        if type == '$b':
            return bool(content)
        elif type == '$i':
            return int(content)
        elif type == '$l':
            return long(content)
        elif type == '$f':
            return float(content)

    def serialize_value(self, value):
        if isinstance(value, bool):
            return '$b%r' % value
        elif isinstance(value, int):
            return '$i%r' % value
        elif isinstance(value, long):
            return '$l%r' % value
        elif isinstance(value, float):
            return '$f%r' % value
        elif isinstance(value, str):
            return value
        elif isinstance(value, dict):
            return self.serialize_columns(value)
        else:
            raise TypeError('Cannot serialize: %s' % type(value))

    def deserialize_columns(self, columns):
        return dict([(key, self.deserialize_value(value)) for key, value in columns.items()])

    def serialize_columns(self, columns):
        return dict([(key, self.serialize_value(value)) for key, value in columns.items()])
        
    def __getattr__(self, item):
        if not item in self.__dict__:
            return getattr(self.delegate, item)


def load_from_settings(settings, prefix='agamemnon.'):
    if settings["%skeyspace" % prefix] == 'memory':
        ds_to_wrap = InMemoryDataStore()
    else:
        ds_to_wrap = CassandraDataStore(settings['%skeyspace' % prefix],
                                        pycassa.connect(settings["%skeyspace" % prefix],
                                                        json.loads(settings["%shost_list" % prefix])),
                                        system_manager=pycassa.system_manager.SystemManager(
                                            json.loads(settings["%shost_list" % prefix])[0]))
    return DataStore(ds_to_wrap)