from itertools import chain, zip_longest
from typing import Dict, List

from sqlalchemy import Column, orm
from werkzeug.datastructures import ImmutableMultiDict

from .const import FilterCategory, FilterType, Query, SortType


class AgGridQuery(orm.Query):
    def sort_filter_by_args(self, args: ImmutableMultiDict, mapper: Dict[str, str] = None):
        """
        querying based on `ImmutableMultiDict` type GET parameters.

        :param args: `ImmutableMultiDict` type GET parameters.
        :param mapper: dict associating GET parameter name (=dict's key) and model class field name (=dict's value).
        :return: self
        """
        return self.sort_filter(
            sort_col_id=args.getlist(Query.SORT_COL_ID),
            sort_type=args.getlist(Query.SORT_TYPE),
            filter_col_id=args.getlist(Query.FILTER_COL_ID),
            filter_type=args.getlist(Query.FILTER_TYPE),
            filter_word=args.getlist(Query.FILTER_WORD),
            filter_to=args.getlist(Query.FILTER_TO),
            filter_category=args.getlist(Query.FILTER_CATEGORY),
            mapper=mapper,
        )

    def sort_filter_by_json(self, json: dict, mapper: Dict[str, str] = None):
        """
        querying base on dict type request form`.

        :param json: dict type key-value (request form as JSON).
        :param mapper: dict associating form field name (=dict's key) and model class field name (=dict's value).
        :return: self
        """
        return self.sort_filter(
            sort_col_id=json.get(Query.SORT_COL_ID, []),
            sort_type=json.get(Query.SORT_TYPE, []),
            filter_col_id=json.get(Query.FILTER_COL_ID, []),
            filter_type=json.get(Query.FILTER_TYPE, []),
            filter_word=json.get(Query.FILTER_WORD, []),
            filter_to=json.get(Query.FILTER_TO, []),
            filter_category=json.get(Query.FILTER_CATEGORY, []),
            mapper=mapper,
        )

    def filter_count_by_args(self, args: ImmutableMultiDict, mapper: Dict[str, str] = None):
        """
        querying based on `ImmutableMultiDict` type GET parameters,
        and return number of records.

        :param args: `ImmutableMultiDict` type GET parameters.
        :param mapper: dict associating GET parameter name (=dict's key) and model class field name (=dict's value).
        :return: self
        """
        return self.filter_count(
            filter_col_id=args.getlist(Query.FILTER_COL_ID),
            filter_type=args.getlist(Query.FILTER_TYPE),
            filter_word=args.getlist(Query.FILTER_WORD),
            filter_to=args.getlist(Query.FILTER_TO),
            filter_category=args.getlist(Query.FILTER_CATEGORY),
            mapper=mapper,
        )

    def filter_count_by_json(self, json: dict, mapper: Dict[str, str] = None):
        """
        querying base on dict type request form,
        and return number of records.

        :param json: dict type key-value (request form as JSON).
        :param mapper: dict associating form field name (=dict's key) and model class field name (=dict's value).
        :return: self
        """
        return self.filter_count(
            filter_col_id=json.get(Query.FILTER_COL_ID, []),
            filter_type=json.get(Query.FILTER_TYPE, []),
            filter_word=json.get(Query.FILTER_WORD, []),
            filter_to=json.get(Query.FILTER_TO, []),
            filter_category=json.get(Query.FILTER_CATEGORY, []),
            mapper=mapper,
        )

    def sort_filter(
        self,
        sort_col_id: List[str],
        sort_type: List[str],
        filter_col_id: List[str],
        filter_type: List[str],
        filter_word: List[str],
        filter_to: List[str],
        filter_category: List[str],
        mapper: Dict[str, str],
    ):
        if mapper is None:
            mapper = {}
        query = self._filter(
            self._map_col_id(filter_col_id, mapper), filter_type, filter_word, filter_to, filter_category
        )

        # sort
        if len(sort_col_id) > 0:

            criterion = [
                self._sort_criterion(s_col, s_type)
                for s_col, s_type in zip(self._map_col_id(sort_col_id, mapper), sort_type)
            ]

            # drop None
            criterion = [cr for cr in criterion if cr is not None]

            query = query.order_by(*criterion)

        return query

    def filter_count(
        self,
        filter_col_id: List[str],
        filter_type: List[str],
        filter_word: List[str],
        filter_to: List[str],
        filter_category: List[str],
        mapper: Dict[str, str],
    ):
        if mapper is None:
            mapper = {}
        return self._filter(
            self._map_col_id(filter_col_id, mapper), filter_type, filter_word, filter_to, filter_category
        ).count()

    def _filter(
        self,
        filter_col_id: List[str],
        filter_type: List[str],
        filter_word: List[str],
        filter_to: List[str],
        filter_category: List[str],
    ):
        query = self

        if len(filter_col_id) > 0:

            # other than 'top' filter
            criterion = [
                self._filter_criterion(f_col, f_category, f_type, f_word, f_to)
                for f_col, f_category, f_type, f_word, f_to in zip_longest(
                    filter_col_id, filter_category, filter_type, filter_word, filter_to
                )
            ]

            # drop None
            criterion = [cr for cr in criterion if cr is not None]

            # flatten
            criterion = list(chain.from_iterable(criterion))

            query = query.filter(*criterion)

            # filter 'top'
            top_filters = [
                (f_col, f_word)
                for f_col, f_category, f_type, f_word, f_to in zip_longest(
                    filter_col_id, filter_category, filter_type, filter_word, filter_to
                )
                if f_category == FilterCategory.RANK and f_type == FilterType.TOP
            ]
            if len(top_filters) > 0:
                for f_col, f_word in top_filters:
                    try:
                        f_number = float(f_word)
                    except ValueError:
                        continue

                    ent = self._query_entity_zero().expr
                    query = query.order_by(getattr(ent, f_col).desc()).limit(f_number).from_self()

        return query

    def _sort_criterion(self, col: str, s_type: str):
        ent = self._query_entity_zero().expr
        column: Column = getattr(ent, col)

        if s_type == SortType.ASC:
            return column.asc()

        if s_type == SortType.DESC:
            return column.desc()

        return None

    def _filter_criterion(self, col: str, f_category: str, f_type: str, f_word: str, f_to: str):
        ent = self._query_entity_zero().expr
        column: Column = getattr(ent, col)

        if f_category == FilterCategory.TEXT:

            if f_type == FilterType.EQUALS:
                return [column == f_word]

            if f_type == FilterType.NOT_EQUAL:
                return [column != f_word]

            if f_type == FilterType.STARTS_WITH:
                return [column.like(f_word + "%")]

            if f_type == FilterType.ENDS_WITH:
                return [column.like("%" + f_word)]

            if f_type == FilterType.CONTAINS:
                return [column.contains(f_word)]

            if f_type == FilterType.NOT_CONTAINS:
                return [~column.contains(f_word)]

            return None

        if f_category == FilterCategory.NUMBER:
            try:
                f_number = float(f_word)
            except ValueError:
                return None

            if f_type == FilterType.EQUALS:
                return [column == f_number]

            if f_type == FilterType.NOT_EQUAL:
                return [column != f_number]

            if f_type == FilterType.LESS_THAN:
                return [column < f_number]

            if f_type == FilterType.LESS_THAN_OR_EQUAL:
                return [column <= f_number]

            if f_type == FilterType.GREATER_THAN:
                return [column > f_number]

            if f_type == FilterType.GREATER_THAN_OR_EQUAL:
                return [column >= f_number]

            if f_type == FilterType.IN_RANGE and f_to is not None:
                try:
                    f_number_to = float(f_to)
                except ValueError:
                    return None
                return [column >= f_number, column <= f_number_to]

            return None

        if f_category == FilterCategory.RANK:
            try:
                f_number = float(f_word)
            except ValueError:
                return None

            if f_type == FilterType.TOP:
                return None  # in case 'top', filter by SQL's LIMIT.

            if f_type == FilterType.GREATER_THAN:
                return [column > f_number]

            if f_type == FilterType.GREATER_THAN_OR_EQUAL:
                return [column >= f_number]

            return None

        return None

    @staticmethod
    def _map_col_id(requested_col_ids: List[str], mapper: Dict[str, str]) -> List[str]:
        def map_if_exist(s: str, d: Dict[str, str]):
            if s in d:
                return d[s]

            return s

        return [map_if_exist(col_id, mapper) for col_id in requested_col_ids]
