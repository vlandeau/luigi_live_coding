import luigi
from luigi import LocalTarget
from luigi.task import ExternalTask, flatten
import csv
import pandas as pd


class CheckRawDataPresence(ExternalTask):
    date = luigi.DateParameter()

    def output(self):
        string_date = self.date.isoformat()
        return LocalTarget("input_data/trivago_raw_%s.csv" % string_date)


class ProcessRawData(luigi.Task):
    date = luigi.DateParameter()
    overwrite_outputs = luigi.BoolParameter(default=False)

    def output(self):
        string_date = self.date.isoformat()
        return LocalTarget("processed_data/trivago_processed_%s.csv" % string_date)

    def requires(self):
        return CheckRawDataPresence(date=self.date)

    def run(self):
        with self.output().open("w") as output_stream:
            writer = csv.writer(output_stream)
            with self.input().open("r") as input_stream:
                reader = csv.reader(input_stream,
                                    delimiter=';')
                new_rows = []
                header = next(reader)
                new_header = header + ['date']
                new_rows.append(new_header)
                string_date = self.date.isoformat()
                for row in reader:
                    new_row = row + [string_date]
                    new_rows.append(new_row)
                writer.writerows(new_rows)

    def complete(self):
        if self.overwrite_outputs:
            outputs = flatten(self.output())

            def _remove_targets(target_list):
                for target in target_list:
                    try:
                        target.remove()
                    except:
                        pass

            _remove_targets(outputs)
            self.overwrite_outputs = False
            return False
        return luigi.Task.complete(self)


class TrainModel(luigi.Task):
    start_date = luigi.DateParameter()
    end_date = luigi.DateParameter()
    overwrite_outputs = luigi.BoolParameter()

    def requires(self):
        date_range = pd.date_range(start=self.start_date,
                                   end=self.end_date)
        requirements = map(lambda date: ProcessRawData(date, self.overwrite_outputs),
                           date_range)
        return requirements

    def run(self):
        pass


if __name__ == '__main__':
    luigi.run()
