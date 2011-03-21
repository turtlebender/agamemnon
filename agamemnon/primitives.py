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

from pycassa.cassandra.ttypes import NotFoundException

__author__ = 'trhowe'

class Relationship(object):
    """
    Represents a directed connection between two nodes
    """

    def __init__(self, rel_id, source_node, target_node, data_store, type, args=dict()):
        """
        Create relationship.
        """
        self._rel_id = rel_id
        self.data_store = data_store
        self._type = type
        self.old_values = args
        self.new_values = {}
        self.relationship_factories = {}
        self._source_node = source_node
        self._target_node = target_node
        self.dirty = False

    @property
    def key(self):
        return self.old_values['rel_key']

    @property
    def rel_key(self):
        return self._rel_id

    @property
    def type(self):
        return self._type

    @property
    def source_node(self):
        return self._source_node

    @property
    def target_node(self):
        return self._target_node

    @property
    def attributes(self):
        for key in self.new_values:
                self.old_values[key] = self.new_values[key]
        return self.old_values

    def __getitem__(self, item):
        if item in self.new_values:
            return self.new_values[item]
        else:
            return self.old_values[item]

    def __setitem__(self, key, value):
        self.new_values[key] = value
        self.dirty = True

    def __delitem__(self, key):
        if key in self.new_values.keys():
            del(self.new_values[key])
        if key in self.old_values.keys():
            del(self.old_values[key])

    def delete(self):
        self.data_store.delete_relationship(self._type, self._rel_id, self.source_node.type, self.source_node.key,
                                            self.target_node.type, self.target_node.key)

    #TODO: fix this
    def commit(self):
        for key in self.new_values:
            self.old_values[key] = self.new_values[key]
        self.data_store.insert(self.type, self.source_node.key, self.old_values, super_key=self._rel_id)

    def __str__(self):
        return '%s: %s -> %s' % (self._type, self.target_node.key, self.target_node.key)

    def __eq__(self, other):
        if not isinstance(other, Relationship):
            return False
        return other.rel_key == self.rel_key

class RelationshipList(object):
    def __init__(self, relationships):
        self._relationships = relationships

    @property
    def single(self):
        if len(self._relationships) > 0:
            return self._relationships[0]
        else:
            return None

    def __len__(self):
        return len(self._relationships)

    def __iter__(self):
        for rel in self._relationships:
            yield rel

class RelationshipFactory(object):
    def __init__(self, data_store, parent_node, rel_type):
        self._rel_type = rel_type
        self._data_store = data_store
        self._parent_node = parent_node

    #TODO: specify order as from key vs timestamp
    def __call__(self, node, key=None, **kwargs):
        return self._data_store.create_relationship(self._rel_type, self._parent_node, node, key,
                                                    dict(**kwargs))


    #TODO: Implement indexing solution here
    def __getitem__(self, item):
        try:
            relationships = []
            for relationship in self:
                if relationship.to_node.key == item:
                    relationships.append(relationship)
            return relationships
        except NotFoundException:
            return []

    
    @property
    def outgoing(self):
        try:
            rels = self._data_store.get_outgoing_relationships(self._parent_node, self._rel_type)
        except NotFoundException:
            rels = []
        return RelationshipList(rels)

    @property
    def incoming(self):
        try:
            rels = self._data_store.get_incoming_relationships(self._parent_node, self._rel_type)
        except NotFoundException:
            rels = []
        return RelationshipList(rels)


    def __len__(self):
        return len(self.outgoing) + len(self.incoming)
    
    def __iter__(self):
        for rel in self.outgoing:
            yield rel
        for rel in self.incoming:
            yield rel


class Node(object):
    def __init__(self, data_store, type, key, args=None):
        self._key = key
        self._data_store = data_store
        self._type = type
        self.old_values = args
        self.new_values = {}
        self.relationship_factories = {}
        self.dirty = False
        self._delete = False

    @property
    def key(self):
        return self._key

    @property
    def type(self):
        return self._type

    def __getattr__(self, item):
        if hasattr(self.__dict__, item):
            return self.__dict__[item]
        else:
            relationship_factory = RelationshipFactory(self._data_store, self, item)
            self.relationship_factories[item] = relationship_factory
            return relationship_factory

    def __getitem__(self, item):
        if item in self.new_values:
            return self.new_values[item]
        else:
            return self.old_values[item]

    def __setitem__(self, key, value):
        self.new_values[key] = value
        self.dirty = True

    def __delitem__(self, key):
        if key in self.new_values.keys():
            del(self.new_values[key])
        if key in self.old_values.keys():
            del(self.old_values[key])
        self.dirty = True

    @property
    def attributes(self):
        attr = {}
        if self.old_values is not None:
            attr.update(self.old_values)
        if self.new_values is not None:
            attr.update(self.new_values)
        return attr

    def delete(self):
        self._data_store.delete_node(self)
        self.node = None

    def commit(self):
        for key in self.new_values:
            self.old_values[key] = self.new_values[key]
        self._data_store.save_node(self)

    def __str__(self):
        return 'Node: %s => %s' % (self.type, self.key)

    def __eq__(self, other):
        if not isinstance(other, Node):
            return False

        return other.key == self.key

