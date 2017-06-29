from time import sleep

import luigi
import pandas as pd
from luigi import LocalTarget
from luigi.task import ExternalTask, flatten

from point_partage.tasks import add_date_to_csv, train_and_save_model


class CheckRawDataPresence(ExternalTask):
    date = luigi.DateParameter()

    def output(self):
        string_date = self.date.isoformat()
        return LocalTarget("input_data/trivago_raw_%s.csv" % string_date)


class ProcessRawData(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        string_date = self.date.isoformat()
        return LocalTarget("processed_data/trivago_processed_%s.csv" % string_date)

    def requires(self):
        return CheckRawDataPresence(date=self.date)

    def run(self):
        add_date_to_csv(self.input(), self.output(), self.date)
        sleep(10)


class TrainModel(luigi.Task):
    start_date = luigi.DateParameter()
    end_date = luigi.DateParameter()

    def requires(self):
        date_range = pd.date_range(start=self.start_date,
                                   end=self.end_date)
        requirements = map(lambda date: ProcessRawData(date),
                           date_range)
        return requirements

    def run(self):
        train_and_save_model(self.input(), self.output())

    def output(self):
        return LocalTarget("models/click_model.dump")


if __name__ == '__main__':
    luigi.run()
