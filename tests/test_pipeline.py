import datetime
import unittest

from luigi.mock import MockTarget

from point_partage.pipeline import CheckRawDataPresence, ProcessRawData


class TestTaskA(unittest.TestCase):
    def test_output_should_return_target_with_good_output_path(self):
        # Given
        process_raw_data = CheckRawDataPresence(
            date=datetime.date(2017, 03, 02))

        # When
        output = process_raw_data.output()

        # Then
        self.assertEquals(output.path, "input_data/trivago_raw_2017-03-02.csv")

    def test_run_should_add_date_column(self):
        # Given
        process_raw_data = FakeProcessingTask(
            date=datetime.date(2017, 03, 02))

        # When
        process_raw_data.run()

        # Then
        with process_raw_data.output().open("r") as output_stream:
            data = output_stream.read()


class FakeProcessingTask(ProcessRawData):
    def output(self):
        return MockTarget("fake_path")
