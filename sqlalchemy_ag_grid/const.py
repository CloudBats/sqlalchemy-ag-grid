class SortType:
    ASC = "asc"
    DESC = "desc"


class FilterCategory:
    TEXT = "text"
    NUMBER = "number"
    RANK = "rank"


class FilterType:
    EQUALS = "equals"
    NOT_EQUAL = "notEqual"
    STARTS_WITH = "startsWith"
    ENDS_WITH = "endsWith"
    CONTAINS = "contains"
    NOT_CONTAINS = "notContains"
    LESS_THAN = "lessThan"
    LESS_THAN_OR_EQUAL = "lessThanOrEqual"
    GREATER_THAN = "greaterThan"
    GREATER_THAN_OR_EQUAL = "greaterThanOrEqual"
    IN_RANGE = "inRange"
    TOP = "top"


class Query:
    SORT_COL_ID = "sortColId"
    SORT_TYPE = "sortType"
    FILTER_COL_ID = "filterColId"
    FILTER_TYPE = "filterType"
    FILTER_WORD = "filterWord"
    FILTER_TO = "filterTo"
    FILTER_CATEGORY = "filterCategory"
