# -*- coding: utf-8 -*-
"""
tests.test_view_number_format
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Scalar rendering for the dash UI: plain decimals inside the readable
range, scientific notation only outside it.
"""

import math

import pytest

from lories.application.view._dash_format import PLAIN_MAX, PLAIN_MIN, format_number


@pytest.mark.parametrize(
    "value, expected",
    [
        (0.0, "0.00"),
        (-0.0, "0.00"),
        (0.001, "0.00100"),
        (0.00123, "0.00123"),
        (0.0123, "0.0123"),
        (0.12, "0.120"),
        (0.5, "0.500"),
        (1.0, "1.00"),
        (12.345, "12.3"),
        (999.9, "1000"),
        (1234.0, "1234"),
        (1234.5678, "1235"),
        (12345.0, "12345"),
        (1234567.0, "1234567"),
        (9876543.21, "9876543"),
        (-1234.0, "-1234"),
        (-0.5, "-0.500"),
        (True, "1.00"),
        (42, "42.0"),
    ],
)
def test_plain_range(value, expected):
    assert format_number(value) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        (0.000999, "9.99e-04"),
        (0.0001234, "1.23e-04"),
        (1e-5, "1.00e-05"),
        (1e7, "1.00e+07"),
        (12345678.0, "1.23e+07"),
        (1.5e9, "1.50e+09"),
        (-2.5e8, "-2.50e+08"),
    ],
)
def test_scientific_outside_range(value, expected):
    assert format_number(value) == expected


def test_boundaries_match_constants():
    assert PLAIN_MIN == 1e-3
    assert PLAIN_MAX == 1e7
    assert "e" not in format_number(PLAIN_MIN)
    assert "e" in format_number(PLAIN_MIN / 2)
    assert "e" in format_number(PLAIN_MAX)
    assert "e" not in format_number(PLAIN_MAX - 1)


def test_non_finite():
    assert format_number(math.nan) == "\u2014"
    assert format_number(math.inf) == "inf"
    assert format_number(-math.inf) == "-inf"


def test_digits_override():
    assert format_number(1234.5678, digits=6) == "1234.57"
    assert format_number(0.5, digits=2) == "0.50"
