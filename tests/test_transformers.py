import unittest

import pandas as pd
from pandas.util.testing import assert_frame_equal
import numpy as np
from point_partage.transformers import NextValueAdder


class TestTransformers(unittest.TestCase):
    def test_next_value_adder(self):
        """[transformers] Test NextValueAdder transformer"""

        # Given
        df = pd.DataFrame({'id_col': [1] * 5 + [2] * 5,
                           'a': range(10),
                           'b': range(10, 20),
                           'c': range(20, 30)})
        df.set_index('id_col', inplace=True)
        expected_res = pd.DataFrame({'id_col': [1] * 5 + [2] * 5,
                                     'a': range(10),
                                     'next_a': range(1, 5) + [np.nan] + range(6, 10) + [np.nan],
                                     'b': range(10, 20),
                                     'next_b': range(11, 15) + [np.nan] + range(16, 20) + [np.nan],
                                     'c': range(20, 30)})
        expected_res.set_index('id_col', inplace=True)

        # When
        df_res = NextValueAdder(cols_to_process=['a', 'b']).fit_transform(df)

        # Then
        assert_frame_equal(df_res, expected_res)
