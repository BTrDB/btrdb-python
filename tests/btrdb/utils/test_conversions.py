# tests.utils.test_conversions
# Testing for btrdb convertion utilities
#
# Author:   PingThings
# Created:  Mon Dec 17 15:23:25 2018 -0500
#
# For license information, see LICENSE.txt
# ID: test_conversions.py [] allen@pingthings.io $

"""
Testing for btrdb convertion utilities
"""

##########################################################################
## Imports
##########################################################################

import uuid
import json
import pytest
import numpy as np

from datetime import datetime
from btrdb.utils.conversion import to_uuid
from btrdb.utils.conversion import AnnotationDecoder, AnnotationEncoder


##########################################################################
## Test Constants
##########################################################################

EXAMPLE_UUID_STR = "07d28a44-4991-492d-b9c5-2d8cec5aa6d4"
EXAMPLE_UUID_BYTES = EXAMPLE_UUID_STR.encode("ASCII")
EXAMPLE_UUID = uuid.UUID(EXAMPLE_UUID_STR)


##########################################################################
## Conversion Tests
##########################################################################

class TestAnnotationJSON(object):

    @pytest.mark.parametrize("obj, expected", [
        (True, "true"),
        (False, "false"),
        (None, "null"),
        (3.14, "3.14"),
        (42, "42"),
        ("foo", "foo"),
        ("a long walk on the beach", "a long walk on the beach"),
        (['a', 'b', 'c'], '["a", "b", "c"]'),
        ({'color': 'red', 'foo': 24}, '{"color": "red", "foo": 24}'),
        (datetime(2018, 9, 10, 16, 30), "2018-09-10 16:30:00.000000"),
        (np.datetime64(datetime(2018, 9, 10, 16, 30)), "2018-09-10 16:30:00.000000+0000"),
        (EXAMPLE_UUID, EXAMPLE_UUID_STR),
    ])
    def test_annotations_encoder(self, obj, expected):
        msg = f"did not correctly encode type {type(obj)}"
        assert json.dumps(obj, cls=AnnotationEncoder, indent=None) == expected, msg

    @pytest.mark.parametrize("obj, s", [
        (True, "true"),
        (False, "false"),
        (None, "null"),
        (3.14, "3.14"),
        (42, "42"),
        ("foo", "foo"),
        ("foo", '"foo"'),
        ("a long walk on the beach", "a long walk on the beach"),
        ("a long walk on the beach", '"a long walk on the beach"'),
        (['a', 'b', 'c'], '["a", "b", "c"]'),
        ({'color': 'red', 'foo': 24}, '{"color": "red", "foo": 24}'),
        ("2018-09-10 16:30:00.000000", "2018-09-10 16:30:00.000000"),
        ({'uuid': EXAMPLE_UUID_STR}, f'{{"uuid": "{EXAMPLE_UUID_STR}"}}'),
    ])
    def test_annotations_decoder(self, obj, s):
        msg = f"did not correctly decode type {type(obj)}"
        assert json.loads(s, cls=AnnotationDecoder) == obj, msg


class TestToUUID(object):

    def test_from_bytes(self):
        """
        Assert that `to_uuid` converts from bytes
        """
        assert to_uuid(EXAMPLE_UUID_BYTES) == EXAMPLE_UUID

    def test_from_str(self):
        """
        Assert that `to_uuid` converts from str
        """
        assert to_uuid(EXAMPLE_UUID_STR) == EXAMPLE_UUID

    def test_from_uuid(self):
        """
        Assert that `to_uuid` returns passed UUID
        """
        assert to_uuid(EXAMPLE_UUID) == EXAMPLE_UUID

    def test_raises_on_bad_data(self):
        """
        Assert that `to_uuid` raises error with bad UUID string
        """
        with pytest.raises(ValueError):
            to_uuid("bad data!!!")

    def test_raises_on_incorrect_input_type(self):
        """
        Assert that `to_uuid` raises error on wrong input class
        """
        with pytest.raises(TypeError) as exc:
            to_uuid(3.0)

        assert "Cannot convert object to UUID" in str(exc)
        assert "float" in str(exc)
