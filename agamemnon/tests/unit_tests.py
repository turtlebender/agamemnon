from contextlib import nested
import unittest
from mock import Mock, patch
from nose.plugins.attrib import attr
import pycassa
from pycassa.cassandra.ttypes import NotFoundException
import graph
import graph.primitives as graph_prim
from graph.graph_constants import ENDPOINT_NAME_TEMPLATE, RELATIONSHIP_KEY_PATTERN, RELATIONSHIP_INDEX

relationship_type = 'pig_cow_alliance'
relationship_types_cf = '__relationship__types'
type_cf = 'test_type'

class BatchMutatorMock(object):
    """
     because we are using the batch.Mutator context manager, we need to implement the magic methods
     __enter__ and __exit__ as they will be called to provide the contextualized object.
     contextualized_batch_mock is that object and is used for all of out assertions
    """

    def __init__(self):
        self._contextualized_batch_mock = Mock()
        self.__enter__ = Mock()
        self.__enter__.return_value = self._contextualized_batch_mock
        self.__exit__ = Mock()

    @property
    def contextualized_batch_mock(self):
        return self._contextualized_batch_mock

class DataStoreTests(unittest.TestCase):
    def setUp(self):
        self._pool = Mock()
        self._system_manager = Mock()
        with patch('pycassa.columnfamily.ColumnFamily') as cf_mock:
            def side_effect(self, name):
                return name

            cf_mock.side_effect = side_effect
            self.store = graph.DataStore('test_space', self._pool, self._system_manager)
        self.pig = graph_prim.Node(self.store, 'test_type', 'pig')
        self.cow = graph_prim.Node(self.store, 'test_type', 'cow')

    @attr(module='graph', type='unit')
    def test_get_non_existent_cf(self):
        with patch('pycassa.columnfamily.ColumnFamily') as cf_mock:
            cf_mock.side_effect = NotFoundException()
            self.failIf(self.store.cf_exists('test_type'))

    @attr(module='graph', type='unit')
    def test_create_node(self):
        #create the node
        with patch('pycassa.columnfamily.ColumnFamily') as cf_mock:
            def new_cf(*args):
                if new_cf.first:
                    new_cf.first = False
                    raise NotFoundException()
                else:
                    return cf_mock.return_value

            new_cf.first = True
            cf_mock.side_effect = new_cf

            def side_effect(name):
                if name is 'test_type':
                    return {'reference': 'reference'}
                else:
                    return Mock()

            cf_mock.return_value.get.side_effect = side_effect
            self.store.create_node('test_type', 'cow', {'sound': 'moo'})
            self._system_manager.create_column_family.assert_called_with('test_space', 'test_type', super=False,
                                                                         comparator_type=pycassa.system_manager.ASCII_TYPE)
            cf_mock.return_value.insert.assert_called_with('cow', {'sound': 'moo'})

    @attr(module='graph', type='unit')
    def test_create_node_existing_cf(self):
        with patch('pycassa.columnfamily.ColumnFamily') as cf_mock:
            def side_effect(name):
                if name is 'test_type':
                    return {'reference': 'reference'}
                else:
                    return Mock()

            cf_mock.return_value.get.side_effect = side_effect
            self.store.create_node('test_type', 'pig', {'sound': 'oink'})
            self.failIf(self._system_manager.create_column_family.called)

    #TODO: unclear what this should really do
    @attr(module='graph', type='unit')
    def test_create_duplicate_node(self):
        with patch('pycassa.columnfamily.ColumnFamily') as cf_mock:
            def side_effect(name):
                if name is 'test_type':
                    return {'reference': 'reference'}
                else:
                    return Mock()

            cf_mock.return_value.get.side_effect = side_effect
            self.store.create_node('test_type', 'pig', {'sound': 'oink'})
            cf_mock.return_value.insert.assert_called_with('pig', {'sound': 'oink'})


    @attr(module='graph', type='unit')
    def test_get_node(self):
        with patch('pycassa.columnfamily.ColumnFamily') as cf_mock:
            cf_mock.return_value.get.return_value = {'sound': 'oink'}
            pig = self.store.get_node('test_type', 'pig')
            cf_mock.return_value.get.assert_called_with('pig')
            self.failUnlessEqual(pig.type, 'test_type')
            self.failUnlessEqual(pig.key, 'pig')
            self.failUnlessEqual(pig['sound'], 'oink')

    @attr(module='graph', type='unit')
    def test_get_non_existent_node(self):
        with patch('pycassa.columnfamily.ColumnFamily') as cf_mock:
            #Test if node is not in datastore
            cf_mock.return_value.get.side_effect = NotFoundException()
            try:
                self.store.get_node('test_type', 'pig')
                self.fail()
            except graph.NodeNotFoundException:
                pass

    @attr(module='graph', type='unit')
    def test_delete_node_simple(self):
        node_mock = Mock()
        node_mock.remove = Mock()
        out_bound_rel_mock = Mock()
        out_bound_rel_mock.get.return_value = {}
        in_bound_rel_mock = Mock()
        in_bound_rel_mock.get.return_value = {}

        def side_effect(pool, type):
            if type == 'outbound__%s' % graph.RELATIONSHIP_INDEX:
                return out_bound_rel_mock
            elif type == 'inbound__%s' % graph.RELATIONSHIP_INDEX:
                return in_bound_rel_mock
            elif type == type_cf:
                return node_mock
            else:
                raise Exception('unknown cf: %s' % type)

        with nested(patch('pycassa.columnfamily.ColumnFamily'),
                    patch('pycassa.batch.Mutator')) as (cf_mock, batch_mock):
            batch_mock.return_value = BatchMutatorMock()
            contextualized_batch_mock = batch_mock.return_value.contextualized_batch_mock
            cf_mock.side_effect = side_effect
            pig = graph_prim.Node(self.store, 'test_type', 'pig')
            pig.delete()
            contextualized_batch_mock.remove.assert_called_with(node_mock, 'pig')


    @attr(module='graph', type='unit')
    def test_create_relationship(self):
        # These are the inserts we expect to get.  The first one is adding friend to the list of relation types
        # the second and third are the to and from relationship inserts.  This is a pretty complicated action.  It
        # executes 7 inserts to create a relationship.  This could be cut down by eliminating the type specific
        # column families, but it is not clear that would make a significant difference in performance
        expected_calls = [
                (('outbound__%s' % RELATIONSHIP_INDEX, ENDPOINT_NAME_TEMPLATE % ('test_type', 'pig'),
                  {RELATIONSHIP_KEY_PATTERN % ('friend', 'pig_cow_alliance'): {'rel_type': 'friend',
                                                                               'rel_key': 'pig_cow_alliance',
                                                                               'source__type': 'test_type',
                                                                               'source__key': 'pig',
                                                                               'source__sound': 'oink',
                                                                               'target__type': 'test_type',
                                                                               'target__key': 'cow',
                                                                               'target__sound': 'moo'}}), {}),

                (('inbound__%s' % RELATIONSHIP_INDEX, ENDPOINT_NAME_TEMPLATE % ('test_type', 'cow'),
                  {'friend__pig_cow_alliance': {'rel_type': 'friend', 'rel_key': 'pig_cow_alliance',
                                                'source__type': 'test_type', 'source__key': 'pig',
                                                'source__sound': 'oink', 'target__type': 'test_type',
                                                'target__key': 'cow', 'target__sound': 'moo'}}), {})]

        pig = graph_prim.Node(self.store, 'test_type', 'pig', {'sound': 'oink'})
        cow = graph_prim.Node(self.store, 'test_type', 'cow', {'sound': 'moo'})
        with nested(patch('pycassa.columnfamily.ColumnFamily'),
                    patch('pycassa.batch.Mutator')) as (cf_mock, batch_mock):
            def side_effect(self, name):
                return name

            cf_mock.side_effect = side_effect

            batch_mock.return_value = BatchMutatorMock()
            contextualized_batch_mock = batch_mock.return_value.contextualized_batch_mock

            ####
            # This is the only method tested in this case.  Crazy.
            ####
            pig.friend(cow, key='pig_cow_alliance')

            # And, let's test our assumptions
            self.failUnlessEqual(contextualized_batch_mock.insert.call_args_list, expected_calls)

    @attr(module='graph', type='unit')
    def test_delete_relationship(self):
        inbound_rel_index_cf = 'inbound__%s' % graph.RELATIONSHIP_INDEX
        outbound_rel_index_cf = 'outbound__%s' % graph.RELATIONSHIP_INDEX
        mocks = {inbound_rel_index_cf: Mock(), outbound_rel_index_cf: Mock()}
        from_key = ENDPOINT_NAME_TEMPLATE % (type_cf, 'pig')
        to_key = ENDPOINT_NAME_TEMPLATE % (type_cf, 'cow')
        relationship_key = 'friend__pig_cow_alliance'

        expected_calls = [((mocks[inbound_rel_index_cf], to_key), {'super_column': relationship_key}),
                          ((mocks[outbound_rel_index_cf], from_key), {'super_column': relationship_key})]

        with nested(patch('pycassa.columnfamily.ColumnFamily'),
                    patch('pycassa.batch.Mutator')) as (cf_mock, batch_mock):
            def side_effect(self, name):
                return mocks[name]

            cf_mock.side_effect = side_effect

            batch_mock.return_value = BatchMutatorMock()
            contextualized_batch_mock = batch_mock.return_value.contextualized_batch_mock
            rel = graph_prim.Relationship('pig_cow_alliance', graph_prim.Node(self.store, 'test_type', 'pig'),
                                          graph_prim.Node(self.store, 'test_type', 'cow'), self.store, 'friend')
            ####
            # Our method under test
            ####
            rel.delete()

            # Test our assumptions
            self.failUnlessEqual(contextualized_batch_mock.remove.call_args_list, expected_calls)

    @attr(module='graph', type='unit')
    def test_get_relationships_of_given_type_outbound_only(self):
        inbound_rel_index_cf = 'inbound__%s' % graph.RELATIONSHIP_INDEX
        outbound_rel_index_cf = 'outbound__%s' % graph.RELATIONSHIP_INDEX
        out_bound_mock = Mock()
        out_bound_mock.get.return_value = {
            'friend__pig_cow_alliance': {'rel_type': 'friend', 'source__type': 'test_type', 'source_key': 'pig',
                                         'target__key': 'cow', 'target__type': 'test_type',
                                         'rel_id': 'pig_cow_alliance'}}
        in_bound_mock = Mock()
        in_bound_mock.get.side_effect = NotFoundException()
        pig = graph_prim.Node(self.store, 'test_type', 'pig')
        cow = graph_prim.Node(self.store, 'test_type', 'cow')

        def side_effect(pool, type):
            if type == outbound_rel_index_cf:
                return out_bound_mock
            elif type == inbound_rel_index_cf:
                return in_bound_mock
            else:
                raise Exception()

        with patch('pycassa.columnfamily.ColumnFamily') as cf_mock:
            cf_mock.side_effect = side_effect

            ####
            # Method under test
            ####
            for rel in pig.friend:
                expected = pig.friend(cow, 'pig_cow_alliance')
                self.failUnlessEqual(expected, rel)

    @attr(module='graph', type='unit')
    def test_get_relationships_of_given_type_inbound_only(self):
        inbound_rel_index_cf = 'inbound__%s' % graph.RELATIONSHIP_INDEX
        outbound_rel_index_cf = 'outbound__%s' % graph.RELATIONSHIP_INDEX
        out_bound_mock = Mock()
        out_bound_mock.get.side_effect = NotFoundException()
        in_bound_mock = Mock()
        in_bound_mock.get.return_value = {
            'friend__chicken_pig_alliance': {'rel_type': 'friend', 'source__type': 'test_type', 'source_key': 'chicken',
                                             'target__key': 'pig', 'target__type': 'test_type',
                                             'rel_id': 'chicken_pig_alliance'}}
        chicken = graph_prim.Node(self.store, 'test_type', 'chicken')
        pig = graph_prim.Node(self.store, 'test_type', 'pig')

        def side_effect(pool, type):
            if type == outbound_rel_index_cf:
                return out_bound_mock
            elif type == inbound_rel_index_cf:
                return in_bound_mock
            else:
                raise Exception()

        with patch('pycassa.columnfamily.ColumnFamily') as cf_mock:
            cf_mock.side_effect = side_effect

            ####
            # Method under test
            ####
            for rel in pig.friend:
                expected = chicken.friend(pig, 'chicken_pig_alliance')
                self.failUnlessEqual(expected, rel)

    @attr(module='graph', type='unit')
    def test_delete_node_with_relationships(self):
        mocks = {'test_type': Mock(), 'outbound__%s' % graph.RELATIONSHIP_INDEX: Mock(),
                 'inbound__%s' % graph.RELATIONSHIP_INDEX: Mock()}
        inbound = 'inbound__%s' % graph.RELATIONSHIP_INDEX
        outbound = 'outbound__%s' % graph.RELATIONSHIP_INDEX
        mocks['test_type'].remove = Mock()
        mocks[inbound].get.side_effect = NotFoundException()

        mocks[outbound].get.return_value = {
            'friend__pig_cow_alliance': {'rel_type': 'friend', 'rel_key': 'pig_cow_alliance',
                                         'source__type': 'test_type', 'source__key': 'pig',
                                         'source__sound': 'oink', 'target__type': 'test_type',
                                         'target__key': 'cow', 'target__sound': 'moo'}}
        from_key = ENDPOINT_NAME_TEMPLATE % ('test_type', 'pig')
        to_key = ENDPOINT_NAME_TEMPLATE % ('test_type', 'cow')
        relationship_key = 'friend__pig_cow_alliance'

        expected_calls = [((mocks[inbound], to_key), {'super_column': relationship_key}),
                          ((mocks[outbound], from_key), {}),
                          ((mocks[inbound], from_key), {}),
                          ((mocks['test_type'], 'pig'), {})]


        def side_effect(pool, type):
            return mocks[type]

        with nested(patch('pycassa.columnfamily.ColumnFamily'),
                    patch('pycassa.batch.Mutator')) as(cf_mock, batch_mock):
            batch_mock.return_value = BatchMutatorMock()
            contextual_batch_mock = batch_mock.return_value.contextualized_batch_mock
            cf_mock.side_effect = side_effect
            self.pig.delete()
            call_args_list = contextual_batch_mock.remove.call_args_list
            self.failUnlessEqual(call_args_list, expected_calls)



