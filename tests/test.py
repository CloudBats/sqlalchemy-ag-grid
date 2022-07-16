from nose2.tools import such
from werkzeug.datastructures import ImmutableMultiDict
from .base_model import Base
from .item_mapper import Item
from .data import data, words
from .session import engine, db_session

# TODO: replace nose2 with pytest

with such.A('sort_filter_by_args()') as it:
    @it.has_setup
    def setup():
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)

        with db_session() as session:
            session.execute(Item.__table__.insert(), data)
            session.commit()

    @it.has_teardown
    def teardown():
        Base.metadata.drop_all(bind=engine)

    @it.should('single sort asc')
    def test1():
        args = ImmutableMultiDict([('sortColId', 'text1'), ('sortType', 'asc')])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = [i.text1 for i in items]
        answer = words * 125
        answer.sort()
        it.assertEqual(answer, target)

    @it.should('single sort desc')
    def test2():
        args = ImmutableMultiDict([('sortColId', 'text1'), ('sortType', 'desc')])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = [i.text1 for i in items]
        answer = words * 125
        answer.sort(reverse=True)
        it.assertEqual(answer, target)

    @it.should('double sort asc, desc')
    def test3():
        args = ImmutableMultiDict([
            ('sortColId', 'text1'),
            ('sortColId', 'number1'),
            ('sortType', 'asc'),
            ('sortType', 'desc'),
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = [(i.text1, i.number1) for i in items]
        answer = []
        for w in words:
            for n in [5, 4, 3, 2, 1]:
                answer.extend([(w, n)] * 25)
        it.assertEqual(answer, target)

    @it.should('single filter text equals')
    def test4():
        args = ImmutableMultiDict([
            ('filterColId', 'text1'),
            ('filterType', 'equals'),
            ('filterWord', 'bravo'),
            ('filterCategory', 'text')
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = [i.text1 for i in items]
        answer = ['bravo'] * 125
        it.assertEqual(answer, target)

    @it.should('filter single text notEqual')
    def test5():
        args = ImmutableMultiDict([
            ('filterColId', 'text1'),
            ('filterType', 'notEqual'),
            ('filterWord', 'bravo'),
            ('filterCategory', 'text'),
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = set([i.text1 for i in items])
        answer = set(words) - {'bravo'}
        it.assertEqual(answer, target)

    @it.should('filter single text startsWith')
    def test6():
        args = ImmutableMultiDict([
            ('filterColId', 'text1'),
            ('filterType', 'startsWith'),
            ('filterWord', 'b'),
            ('filterCategory', 'text'),
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = set([i.text1 for i in items])
        answer = {'bravo'}
        it.assertEqual(answer, target)

    @it.should('filter single text endsWith')
    def test7():
        args = ImmutableMultiDict([
            ('filterColId', 'text1'),
            ('filterType', 'endsWith'),
            ('filterWord', 'o'),
            ('filterCategory', 'text'),
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = set([i.text1 for i in items])
        answer = {'bravo', 'echo'}
        it.assertEqual(answer, target)

    @it.should('filter single text contains')
    def test8():
        args = ImmutableMultiDict([
            ('filterColId', 'text1'),
            ('filterType', 'contains'),
            ('filterWord', 'ch'),
            ('filterCategory', 'text'),
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = set([i.text1 for i in items])
        answer = {'charlie', 'echo'}
        it.assertEqual(answer, target)

    @it.should('filter single text notContains')
    def test9():
        args = ImmutableMultiDict([
            ('filterColId', 'text1'),
            ('filterType', 'notContains'),
            ('filterWord', 'ch'),
            ('filterCategory', 'text'),
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = set([i.text1 for i in items])
        answer = set(words) - {'charlie', 'echo'}
        it.assertEqual(answer, target)

    @it.should('filter single number equals')
    def test10():
        args = ImmutableMultiDict([
            ('filterColId', 'number1'),
            ('filterType', 'equals'),
            ('filterWord', 1),
            ('filterCategory', 'number')
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = set([i.number1 for i in items])
        answer = {1}
        it.assertEqual(answer, target)

    @it.should('filter single number notEqual')
    def test11():
        args = ImmutableMultiDict([
            ('filterColId', 'number1'),
            ('filterType', 'notEqual'),
            ('filterWord', 1),
            ('filterCategory', 'number')
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = set([i.number1 for i in items])
        answer = {2, 3, 4, 5}
        it.assertEqual(answer, target)

    @it.should('filter single number lessThan')
    def test12():
        args = ImmutableMultiDict([
            ('filterColId', 'number1'),
            ('filterType', 'lessThan'),
            ('filterWord', 3),
            ('filterCategory', 'number')
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = set([i.number1 for i in items])
        answer = {1, 2}
        it.assertEqual(answer, target)

    @it.should('filter single number lessThanOrEqual')
    def test13():
        args = ImmutableMultiDict([
            ('filterColId', 'number1'),
            ('filterType', 'lessThanOrEqual'),
            ('filterWord', 3),
            ('filterCategory', 'number')
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = set([i.number1 for i in items])
        answer = {1, 2, 3}
        it.assertEqual(answer, target)

    @it.should('filter single number greaterThan')
    def test14():
        args = ImmutableMultiDict([
            ('filterColId', 'number1'),
            ('filterType', 'greaterThan'),
            ('filterWord', 3),
            ('filterCategory', 'number')
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = set([i.number1 for i in items])
        answer = {4, 5}
        it.assertEqual(answer, target)

    @it.should('filter single number greaterThanOrEqual')
    def test15():
        args = ImmutableMultiDict([
            ('filterColId', 'number1'),
            ('filterType', 'greaterThanOrEqual'),
            ('filterWord', 3),
            ('filterCategory', 'number')
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = set([i.number1 for i in items])
        answer = {3, 4, 5}
        it.assertEqual(answer, target)

    @it.should('filter single number inRange')
    def test16():
        args = ImmutableMultiDict([
            ('filterColId', 'number1'),
            ('filterType', 'inRange'),
            ('filterWord', 2),
            ('filterTo', 4),
            ('filterCategory', 'number')
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = set([i.number1 for i in items])
        answer = {2, 3, 4}
        it.assertEqual(answer, target)

    @it.should('filter single rank top')
    def test17():
        args = ImmutableMultiDict([
            ('filterColId', 'number1'),
            ('filterType', 'top'),
            ('filterWord', 100),
            ('filterCategory', 'rank'),
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = set([i.number1 for i in items])
        answer = {5}
        it.assertEqual(answer, target)
        it.assertEqual(len(items), 100)

    @it.should('filter single rank greaterThan')
    def test18():
        args = ImmutableMultiDict([
            ('filterColId', 'number1'),
            ('filterType', 'greaterThan'),
            ('filterWord', 3),
            ('filterCategory', 'rank'),
    ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = set([i.number1 for i in items])
        answer = {4, 5}
        it.assertEqual(answer, target)

    @it.should('filter single rank greaterThanOrEqual')
    def test19():
        args = ImmutableMultiDict([
            ('filterColId', 'number1'),
            ('filterType', 'greaterThanOrEqual'),
            ('filterWord', 3),
            ('filterCategory', 'rank'),
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = set([i.number1 for i in items])
        answer = {3, 4, 5}
        it.assertEqual(answer, target)

    @it.should('filter double text equals and number greaterThan')
    def test20():
        args = ImmutableMultiDict([
            ('filterColId', 'text1'),
            ('filterType', 'equals'),
            ('filterWord', 'bravo'),
            ('filterCategory', 'text'),
            ('filterColId', 'number1'),
            ('filterType', 'greaterThan'),
            ('filterWord', 3),
            ('filterCategory', 'number'),
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target1 = set([i.text1 for i in items])
        answer1 = {'bravo'}
        target2 = set([i.number1 for i in items])
        answer2 = {4, 5}
        it.assertEqual(answer1, target1)
        it.assertEqual(answer2, target2)

    @it.should('filter double text equals and number inRange')
    def test21():
        args = ImmutableMultiDict([
            ('filterColId', 'text1'),
            ('filterType', 'equals'),
            ('filterWord', 'bravo'),
            ('filterTo', None),
            ('filterCategory', 'text'),
            ('filterColId', 'number1'),
            ('filterType', 'inRange'),
            ('filterWord', 2),
            ('filterTo', 4),
            ('filterCategory', 'number'),
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target1 = set([i.text1 for i in items])
        answer1 = {'bravo'}
        target2 = set([i.number1 for i in items])
        answer2 = {2, 3, 4}
        it.assertEqual(answer1, target1)
        it.assertEqual(answer2, target2)

    @it.should('filter double text equals and rank top')
    def test22():
        args = ImmutableMultiDict([
            ('filterColId', 'text1'),
            ('filterType', 'equals'),
            ('filterWord', 'bravo'),
            ('filterCategory', 'text'),
            ('filterColId', 'number1'),
            ('filterType', 'top'),
            ('filterWord', 10),
            ('filterCategory', 'rank'),
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = ['{0}{1}'.format(i.text1, i.number1) for i in items]
        answer = ['bravo5'] * 10
        it.assertEqual(answer, target)

    @it.should('filter double rank top and text equals')
    def test23():
        args = ImmutableMultiDict([
            ('filterColId', 'number1'),
            ('filterType', 'top'),
            ('filterWord', '10'),
            ('filterCategory', 'rank'),
            ('filterColId', 'text1'),
            ('filterType', 'equals'),
            ('filterWord', 'bravo'),
            ('filterCategory', 'text'),
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = ['{0}{1}'.format(i.text1, i.number1) for i in items]
        answer = ['bravo5'] * 10
        it.assertEqual(answer, target)

    @it.should('filter and sort')
    def test24():
        args = ImmutableMultiDict([
            ('sortColId', 'text1'),
            ('sortType', 'asc'),
            ('sortColId', 'number1'),
            ('sortType', 'desc'),
            ('filterColId', 'text1'),
            ('filterType', 'contains'),
            ('filterWord', 'ch'),
            ('filterCategory', 'text'),
            ('filterColId', 'number1'),
            ('filterType', 'greaterThan'),
            ('filterWord', 3),
            ('filterCategory', 'number'),
        ])
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args).all()

        target = set(['{0}{1}'.format(i.text1, i.number1) for i in items])
        answer = {'echo5', 'echo4', 'charlie5', 'charlie4'}
        it.assertEqual(answer, target)

    @it.should('mapper')
    def test25():
        args = ImmutableMultiDict([
            ('filterColId', 'text_1'),
            ('filterType', 'equals'),
            ('filterWord', 'bravo'),
            ('filterCategory', 'text'),
            ('filterColId', 'number_1'),
            ('filterType', 'greaterThan'),
            ('filterWord', 3),
            ('filterCategory', 'number'),
        ])
        mapper = {
            'text_1': 'text1',
            'number_1': 'number1',
        }
        with db_session() as session:
            items = session.query(Item).sort_filter_by_args(args, mapper).all()

        target = set(['{}{}'.format(i.text1, i.number1) for i in items])
        answer = {'bravo4', 'bravo5'}
        it.assertEqual(answer, target)

    it.createTests(globals())

with such.A('sort_filter_by_json()') as it:
    @it.has_setup
    def setup():
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)

        with db_session() as session:
            session.execute(Item.__table__.insert(), data)
            session.commit()

    @it.has_teardown
    def teardown():
        Base.metadata.drop_all(bind=engine)

    @it.should('single sort')
    def test26():
        json = dict(
            sortColId=['text1'],
            sortType=['asc'],
        )
        with db_session() as session:
            items = session.query(Item).sort_filter_by_json(json).all()
        target = [i.text1 for i in items]
        answer = words * 125
        answer.sort()
        it.assertEqual(answer, target)

    @it.should('second inRange')
    def test27():
        json = dict(
            filterColId=['text1', 'number1'],
            filterType=['equals', 'inRange'],
            filterWord=['bravo', 2],
            filterTo=[None, 4],
            filterCategory=['text', 'number'],
        )
        with db_session() as session:
            items = session.query(Item).sort_filter_by_json(json).all()

        target1 = set([i.text1 for i in items])
        answer1 = {'bravo'}
        target2 = set([i.number1 for i in items])
        answer2 = {2, 3, 4}
        it.assertEqual(answer1, target1)
        it.assertEqual(answer2, target2)

    it.createTests(globals())

with such.A('filter_counter_by_args') as it:
    @it.has_setup
    def setup():
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)

        with db_session() as session:
            session.execute(Item.__table__.insert(), data)
            session.commit()

    @it.has_teardown
    def teardown():
        Base.metadata.drop_all(bind=engine)

    @it.should('single filter text equals')
    def test28():
        args = ImmutableMultiDict([
            ('filterColId', 'text1'),
            ('filterType', 'equals'),
            ('filterWord', 'bravo'),
            ('filterCategory', 'text'),
        ])
        with db_session() as session:
            target = session.query(Item).filter_count_by_args(args)
        it.assertEqual(target, 125)

    @it.should('double filter text equals and number greaterThan')
    def test29():
        args = ImmutableMultiDict([
            ('filterColId', 'text1'),
            ('filterType', 'equals'),
            ('filterWord', 'bravo'),
            ('filterCategory', 'text'),
            ('filterColId', 'number1'),
            ('filterType', 'greaterThan'),
            ('filterWord', 3),
            ('filterCategory', 'number'),
        ])
        with db_session() as session:
            target = session.query(Item).filter_count_by_args(args)
        it.assertEqual(target, 50)

    it.createTests(globals())

with such.A('filter_counter_by_json') as it:
    @it.has_setup
    def setup():
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)

        with db_session() as session:
            session.execute(Item.__table__.insert(), data)
            session.commit()

    @it.has_teardown
    def teardown():
        Base.metadata.drop_all(bind=engine)


    @it.should('single filter text equals')
    def test30():
        json = dict(
            filterColId=['text1'],
            filterType=['equals'],
            filterWord=['bravo'],
            filterCategory=['text']
        )
        with db_session() as session:
            target = session.query(Item).filter_count_by_json(json)
        it.assertEqual(target, 125)

    @it.should('double filter text equals and number greaterThan')
    def test31():
        json = dict(
            filterColId=['text1', 'number1'],
            filterType=['equals', 'greaterThan'],
            filterWord=['bravo', 3],
            filterCategory=['text', 'number'],
        )
        with db_session() as session:
            target = session.query(Item).filter_count_by_json(json)
        it.assertEqual(target, 50)

    it.createTests(globals())
