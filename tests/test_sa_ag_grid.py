from werkzeug.datastructures import ImmutableMultiDict

from .data import words
from .item import Item


def test_single_sort_asc(insert_data, db_session):
    args = ImmutableMultiDict([("sortColId", "text1"), ("sortType", "asc")])
    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = [i.text1 for i in items]
    answer = words * 125
    answer.sort()
    assert answer == target


def test_single_sort_desc(insert_data, db_session):
    args = ImmutableMultiDict([("sortColId", "text1"), ("sortType", "desc")])
    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = [i.text1 for i in items]
    answer = words * 125
    answer.sort(reverse=True)
    assert answer == target


def test_double_sort_asc_desc(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("sortColId", "text1"),
            ("sortColId", "number1"),
            ("sortType", "asc"),
            ("sortType", "desc"),
        ]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = [(i.text1, i.number1) for i in items]
    answer = []
    for w in words:
        for n in [5, 4, 3, 2, 1]:
            answer.extend([(w, n)] * 25)
    assert answer == target


def test_single_filter_text_equals(insert_data, db_session):
    args = ImmutableMultiDict(
        [("filterColId", "text1"), ("filterType", "equals"), ("filterWord", "bravo"), ("filterCategory", "text")]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = [i.text1 for i in items]
    answer = ["bravo"] * 125
    assert answer == target


def test_filter_single_text_not_equal(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("filterColId", "text1"),
            ("filterType", "notEqual"),
            ("filterWord", "bravo"),
            ("filterCategory", "text"),
        ]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = set([i.text1 for i in items])
    answer = set(words) - {"bravo"}
    assert answer == target


def test_filter_single_text_starts_with(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("filterColId", "text1"),
            ("filterType", "startsWith"),
            ("filterWord", "b"),
            ("filterCategory", "text"),
        ]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = set([i.text1 for i in items])
    answer = {"bravo"}
    assert answer == target


def test_filter_single_text_ends_with(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("filterColId", "text1"),
            ("filterType", "endsWith"),
            ("filterWord", "o"),
            ("filterCategory", "text"),
        ]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = set([i.text1 for i in items])
    answer = {"bravo", "echo"}
    assert answer == target


def test_single_text_contains(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("filterColId", "text1"),
            ("filterType", "contains"),
            ("filterWord", "ch"),
            ("filterCategory", "text"),
        ]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = set([i.text1 for i in items])
    answer = {"charlie", "echo"}
    assert answer == target


def test_filter_single_text_not_contains(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("filterColId", "text1"),
            ("filterType", "notContains"),
            ("filterWord", "ch"),
            ("filterCategory", "text"),
        ]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = set([i.text1 for i in items])
    answer = set(words) - {"charlie", "echo"}
    assert answer == target


def test_filter_single_number_equals(insert_data, db_session):
    args = ImmutableMultiDict(
        [("filterColId", "number1"), ("filterType", "equals"), ("filterWord", 1), ("filterCategory", "number")]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = set([i.number1 for i in items])
    answer = {1}
    assert answer == target


def test_filter_number_not_equal(insert_data, db_session):
    args = ImmutableMultiDict(
        [("filterColId", "number1"), ("filterType", "notEqual"), ("filterWord", 1), ("filterCategory", "number")]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = set([i.number1 for i in items])
    answer = {2, 3, 4, 5}
    assert answer == target


def test_filter_single_number_less_than(insert_data, db_session):
    args = ImmutableMultiDict(
        [("filterColId", "number1"), ("filterType", "lessThan"), ("filterWord", 3), ("filterCategory", "number")]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = set([i.number1 for i in items])
    answer = {1, 2}
    assert answer == target


def test_filter_single_number_less_than_or_equal(insert_data, db_session):
    args = ImmutableMultiDict(
        [("filterColId", "number1"), ("filterType", "lessThanOrEqual"), ("filterWord", 3), ("filterCategory", "number")]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = set([i.number1 for i in items])
    answer = {1, 2, 3}
    assert answer == target


def test_filter_single_number_greater_than(insert_data, db_session):
    args = ImmutableMultiDict(
        [("filterColId", "number1"), ("filterType", "greaterThan"), ("filterWord", 3), ("filterCategory", "number")]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = set([i.number1 for i in items])
    answer = {4, 5}
    assert answer == target


def test_filter_single_number_greater_than_or_equal(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("filterColId", "number1"),
            ("filterType", "greaterThanOrEqual"),
            ("filterWord", 3),
            ("filterCategory", "number"),
        ]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = set([i.number1 for i in items])
    answer = {3, 4, 5}
    assert answer == target


def test_filter_single_number_in_range(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("filterColId", "number1"),
            ("filterType", "inRange"),
            ("filterWord", 2),
            ("filterTo", 4),
            ("filterCategory", "number"),
        ]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = set([i.number1 for i in items])
    answer = {2, 3, 4}
    assert answer == target


def test_filter_single_rank_top(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("filterColId", "number1"),
            ("filterType", "top"),
            ("filterWord", 100),
            ("filterCategory", "rank"),
        ]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = set([i.number1 for i in items])
    answer = {5}
    assert answer == target
    assert len(items) == 100


def test_filter_single_rank_greater_than(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("filterColId", "number1"),
            ("filterType", "greaterThan"),
            ("filterWord", 3),
            ("filterCategory", "rank"),
        ]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = set([i.number1 for i in items])
    answer = {4, 5}
    assert answer == target


def test_filter_single_rank_greater_than_or_equal(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("filterColId", "number1"),
            ("filterType", "greaterThanOrEqual"),
            ("filterWord", 3),
            ("filterCategory", "rank"),
        ]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = set([i.number1 for i in items])
    answer = {3, 4, 5}
    assert answer == target


def test_filter_double_text_equals_and_number_greater_than(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("filterColId", "text1"),
            ("filterType", "equals"),
            ("filterWord", "bravo"),
            ("filterCategory", "text"),
            ("filterColId", "number1"),
            ("filterType", "greaterThan"),
            ("filterWord", 3),
            ("filterCategory", "number"),
        ]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target1 = set([i.text1 for i in items])
    answer1 = {"bravo"}
    target2 = set([i.number1 for i in items])
    answer2 = {4, 5}
    assert answer1 == target1
    assert answer2 == target2


def test_filter_double_text_equals_and_number_in_range(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("filterColId", "text1"),
            ("filterType", "equals"),
            ("filterWord", "bravo"),
            ("filterTo", None),
            ("filterCategory", "text"),
            ("filterColId", "number1"),
            ("filterType", "inRange"),
            ("filterWord", 2),
            ("filterTo", 4),
            ("filterCategory", "number"),
        ]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target1 = set([i.text1 for i in items])
    answer1 = {"bravo"}
    target2 = set([i.number1 for i in items])
    answer2 = {2, 3, 4}
    assert answer1 == target1
    assert answer2 == target2


def test_filter_double_text_equals_and_rank_top(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("filterColId", "text1"),
            ("filterType", "equals"),
            ("filterWord", "bravo"),
            ("filterCategory", "text"),
            ("filterColId", "number1"),
            ("filterType", "top"),
            ("filterWord", 10),
            ("filterCategory", "rank"),
        ]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = ["{0}{1}".format(i.text1, i.number1) for i in items]
    answer = ["bravo5"] * 10
    assert answer == target


def test_filter_double_rank_top_and_text_equals(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("filterColId", "number1"),
            ("filterType", "top"),
            ("filterWord", "10"),
            ("filterCategory", "rank"),
            ("filterColId", "text1"),
            ("filterType", "equals"),
            ("filterWord", "bravo"),
            ("filterCategory", "text"),
        ]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = ["{0}{1}".format(i.text1, i.number1) for i in items]
    answer = ["bravo5"] * 10
    assert answer == target


def test_filter_and_sort(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("sortColId", "text1"),
            ("sortType", "asc"),
            ("sortColId", "number1"),
            ("sortType", "desc"),
            ("filterColId", "text1"),
            ("filterType", "contains"),
            ("filterWord", "ch"),
            ("filterCategory", "text"),
            ("filterColId", "number1"),
            ("filterType", "greaterThan"),
            ("filterWord", 3),
            ("filterCategory", "number"),
        ]
    )

    items = db_session.query(Item).sort_filter_by_args(args).all()

    target = set(["{0}{1}".format(i.text1, i.number1) for i in items])
    answer = {"echo5", "echo4", "charlie5", "charlie4"}
    assert answer == target


def test_mapper(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("filterColId", "text_1"),
            ("filterType", "equals"),
            ("filterWord", "bravo"),
            ("filterCategory", "text"),
            ("filterColId", "number_1"),
            ("filterType", "greaterThan"),
            ("filterWord", 3),
            ("filterCategory", "number"),
        ]
    )
    mapper = {
        "text_1": "text1",
        "number_1": "number1",
    }

    items = db_session.query(Item).sort_filter_by_args(args, mapper).all()

    target = set(["{}{}".format(i.text1, i.number1) for i in items])
    answer = {"bravo4", "bravo5"}
    assert answer == target


def test_single_sort_json(insert_data, db_session):
    json = dict(
        sortColId=["text1"],
        sortType=["asc"],
    )
    items = db_session.query(Item).sort_filter_by_json(json).all()
    target = [i.text1 for i in items]
    answer = words * 125
    answer.sort()
    assert answer == target


def test_second_in_range_json(insert_data, db_session):
    json = dict(
        filterColId=["text1", "number1"],
        filterType=["equals", "inRange"],
        filterWord=["bravo", 2],
        filterTo=[None, 4],
        filterCategory=["text", "number"],
    )

    items = db_session.query(Item).sort_filter_by_json(json).all()

    target1 = set([i.text1 for i in items])
    answer1 = {"bravo"}
    target2 = set([i.number1 for i in items])
    answer2 = {2, 3, 4}
    assert answer1 == target1
    assert answer2 == target2


def test_filter_counter_single_filter_text_equals(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("filterColId", "text1"),
            ("filterType", "equals"),
            ("filterWord", "bravo"),
            ("filterCategory", "text"),
        ]
    )

    target = db_session.query(Item).filter_count_by_args(args)
    assert target == 125


def test_filter_counter_double_filter_text_equals_and_number_greater_than(insert_data, db_session):
    args = ImmutableMultiDict(
        [
            ("filterColId", "text1"),
            ("filterType", "equals"),
            ("filterWord", "bravo"),
            ("filterCategory", "text"),
            ("filterColId", "number1"),
            ("filterType", "greaterThan"),
            ("filterWord", 3),
            ("filterCategory", "number"),
        ]
    )

    target = db_session.query(Item).filter_count_by_args(args)
    assert target == 50


def test_filter_counter_json_single_filter_text_equals(insert_data, db_session):
    json = dict(filterColId=["text1"], filterType=["equals"], filterWord=["bravo"], filterCategory=["text"])

    target = db_session.query(Item).filter_count_by_json(json)
    assert target == 125


def test_filter_counter_json_double_filter_text_equals_and_number_greater_than(insert_data, db_session):
    json = dict(
        filterColId=["text1", "number1"],
        filterType=["equals", "greaterThan"],
        filterWord=["bravo", 3],
        filterCategory=["text", "number"],
    )

    target = db_session.query(Item).filter_count_by_json(json)
    assert target == 50
