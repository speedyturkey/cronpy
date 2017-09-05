import datetime
import unittest
from unittest import mock
from cronpy.cronpy import default_cron


class mock_datetime(object):
    """
    Monkey-patch datetime for predictable results
    """
    def __init__(self, year, month, day, hour, minute):
        self.year = year
        self.month = month
        self.day = day
        self.hour = hour
        self.minute = minute

    def __enter__(self):
        class MockDate(datetime.datetime):
            @classmethod
            def today(cls):
                return cls(self.year, self.month, self.day)

            @classmethod
            def now(cls):
                return cls(self.year, self.month, self.day,
                           self.hour, self.minute)
        self.original_datetime = datetime.datetime
        datetime.datetime = MockDate

    def __exit__(self, *args, **kwargs):
        datetime.datetime = self.original_datetime


def make_mock_task(name=None):
    task = mock.Mock()
    task.__name__ = name or 'task'
    return task


class SchedulerTests(unittest.TestCase):

    def setUp(self):
        del default_cron.tasks[:]

    def test_at(self):
        mock_task = make_mock_task()
        with mock_datetime(2017, 9, 4, 12, 00):
            self.assertEqual(default_cron.new_task().every_day().at("11:30").do(mock_task).next_run_time, datetime.time(11, 30))
            self.assertEqual(default_cron.new_task().at("11:30").do(mock_task).next_run_time, datetime.time(11, 30))
        with self.assertRaises(AssertionError):
            default_cron.new_task().at("09:60").do(job)
        with self.assertRaises(AssertionError):
            default_cron.new_task().at("24:59").do(job)

    def test_on_days(self):
        self.assertEqual(default_cron.new_task().on_days("0-2").days, [0, 1, 2])
        self.assertEqual(default_cron.new_task().on_days("0 - 2").days, [0, 1, 2])
        self.assertEqual(default_cron.new_task().on_days("0:4").days, [0, 1, 2, 3, 4])
        self.assertEqual(default_cron.new_task().on_days("0, 1").days, [0, 1])
        self.assertEqual(default_cron.new_task().on_days("0, 5").days, [0, 5])
        self.assertEqual(default_cron.new_task().on_days("0, 2, 4").days, [0, 2, 4])
        self.assertEqual(default_cron.new_task().on_days("6").days, [6])
        with self.assertRaises(ValueError):
            default_cron.new_task().on_days("0-2; 3")
        with self.assertRaises(ValueError):
            default_cron.new_task().on_days("5-7")
        with self.assertRaises(ValueError):
            default_cron.new_task().on_days("0-")
        with self.assertRaises(ValueError):
            default_cron.new_task().on_days("4-3")

    def test_at_hours(self):
        self.assertEqual(default_cron.new_task().at_hours("0-5").hours, [0, 1, 2, 3, 4, 5])
        self.assertEqual(default_cron.new_task().at_hours("0 - 5").hours, [0, 1, 2, 3, 4, 5])
        self.assertEqual(default_cron.new_task().at_hours("9:14").hours, [9, 10, 11, 12, 13, 14])
        self.assertEqual(default_cron.new_task().at_hours("0, 12").hours, [0, 12])
        self.assertEqual(default_cron.new_task().at_hours("4, 16").hours, [4, 16])
        self.assertEqual(default_cron.new_task().at_hours("6, 12, 18").hours, [6, 12, 18])
        self.assertEqual(default_cron.new_task().at_hours("22").hours, [22])
        with self.assertRaises(ValueError):
            default_cron.new_task().at_hours("0-4; 12")
        with self.assertRaises(AssertionError):
            default_cron.new_task().at_hours("22-26")
        with self.assertRaises(ValueError):
            default_cron.new_task().at_hours("12-")
        with self.assertRaises(ValueError):
            default_cron.new_task().at_hours("14-10")

    def test_at_minutes(self):
        self.assertEqual(default_cron.new_task().at_minutes("0-5").minutes, [0, 1, 2, 3, 4, 5])
        self.assertEqual(default_cron.new_task().at_minutes("0 - 5").minutes, [0, 1, 2, 3, 4, 5])
        self.assertEqual(default_cron.new_task().at_minutes("0:4").minutes, [0, 1, 2, 3, 4])
        self.assertEqual(default_cron.new_task().at_minutes("0, 30").minutes, [0, 30])
        self.assertEqual(default_cron.new_task().at_minutes("15, 45").minutes, [15, 45])
        self.assertEqual(default_cron.new_task().at_minutes("0, 20, 40").minutes, [0, 20, 40])
        self.assertEqual(default_cron.new_task().at_minutes("28").minutes, [28])
        with self.assertRaises(ValueError):
            default_cron.new_task().at_minutes("0-30; 45")
        with self.assertRaises(AssertionError):
            default_cron.new_task().at_minutes("55-65")
        with self.assertRaises(ValueError):
            default_cron.new_task().at_minutes("0-")
        with self.assertRaises(ValueError):
            default_cron.new_task().at_minutes("45-30")

    def test_on_days_at_hours_and_minutes(self):
        task = default_cron.new_task().monday().at_hours("9:12").at_minutes("15, 45")
        self.assertEqual(task.days, [0])
        self.assertEqual(task.hours, [9, 10, 11, 12])
        self.assertEqual(task.minutes, [15, 45])
        task = default_cron.new_task().monday().at_hours("9 - 12").at_minutes("15, 45")
        self.assertEqual(task.days, [0])
        self.assertEqual(task.hours, [9, 10, 11, 12])
        self.assertEqual(task.minutes, [15, 45])
        task = default_cron.new_task().on_days("0, 2, 4").at_hours("6, 12, 18").at_minutes("15, 45")
        self.assertEqual(task.days, [0, 2, 4])
        self.assertEqual(task.hours, [6, 12, 18])
        self.assertEqual(task.minutes, [15, 45])
        task = default_cron.new_task().on_days("0-4").at_minutes("28-32")
        self.assertEqual(task.days, [0, 1, 2, 3, 4])
        self.assertEqual(task.hours, list(range(0, 24)))
        self.assertEqual(task.minutes, [28, 29, 30, 31, 32])

    def test_day_functions(self):
        self.assertEqual(default_cron.new_task().monday().days, [0])
        self.assertEqual(default_cron.new_task().sunday().days, [6])
        self.assertEqual(default_cron.new_task().monday().wednesday().friday().days, [0, 2, 4])

    def test_run_on_days_at_time(self):
        mock_task = make_mock_task()
        with mock_datetime(2017, 9, 4, 12, 00):
            self.assertEqual(default_cron.new_task().every_day().at("12:30").do(mock_task).next_run_time, datetime.time(12, 30))
            self.assertEqual(default_cron.new_task().every_day().at("11:30").do(mock_task).next_run_time, datetime.time(11, 30))
            self.assertEqual(default_cron.new_task().every_day().at("11:30").do(mock_task).next_run_date, datetime.date(2017, 9, 5))
            self.assertEqual(default_cron.new_task().on_days("1-4").at("12:30").do(mock_task).next_run_date, datetime.date(2017, 9, 5))
            self.assertEqual(default_cron.new_task().on_days("5-6").at("12:30").do(mock_task).next_run_date, datetime.date(2017, 9, 9))

    def test_hour_and_minute_intervals(self):
        mock_task = make_mock_task()
        task = default_cron.new_task().every_day().hour_interval(6).minute_interval(15).do(mock_task)
        self.assertEqual(task.hours, [0, 6, 12, 18])
        self.assertEqual(task.minutes, [0, 15, 30, 45])
        with self.assertRaises(AssertionError):
            default_cron.new_task().every_day().hour_interval(25).do(mock_task)
        with self.assertRaises(AssertionError):
            default_cron.new_task().every_day().minute_interval(61).do(mock_task)

    def test_run_at_specified_time_starting_today(self):
        mock_task = make_mock_task()
        with mock_datetime(2017, 9, 4, 12, 0):
            default_cron.new_task().at("13:00").do(mock_task)
            default_cron.run_pending()
            self.assertEqual(mock_task.call_count, 0)

        with mock_datetime(2017, 9, 4, 13, 1):
            default_cron.run_pending()
            self.assertEqual(mock_task.call_count, 1)

        with mock_datetime(2017, 9, 5, 13, 1):
            default_cron.run_pending()
            self.assertEqual(mock_task.call_count, 2)

    def test_run_on_specified_days_past_today(self):
        mock_task = make_mock_task()
        with mock_datetime(2017, 9, 4, 12, 16):
            default_cron.new_task().on_days("0,2,4").at('12:15').do(mock_task)
            default_cron.run_pending()
            assert mock_task.call_count == 0

        with mock_datetime(2017, 9, 4, 12, 16):
            default_cron.run_pending()
            assert mock_task.call_count == 0

        with mock_datetime(2017, 9, 5, 12, 16):
            default_cron.run_pending()
            assert mock_task.call_count == 0

        with mock_datetime(2017, 9, 6, 12, 16):
            default_cron.run_pending()
            assert mock_task.call_count == 1

    def test_run_at_specified_hours(self):
        mock_task = make_mock_task()
        with mock_datetime(2017, 9, 4, 9, 0):
            default_cron.new_task().at_hours("12-16").do(mock_task)
            default_cron.run_pending()
            assert mock_task.call_count == 0
        with mock_datetime(2017, 9, 4, 11, 0):
            default_cron.run_pending()
            assert mock_task.call_count == 0
        with mock_datetime(2017, 9, 4, 12, 1):
            default_cron.run_pending()
            assert mock_task.call_count == 1
        with mock_datetime(2017, 9, 4, 13, 1):
            default_cron.run_pending()
            assert mock_task.call_count == 2
        with mock_datetime(2017, 9, 4, 15, 1):
            default_cron.run_pending()
            assert mock_task.call_count == 3
        with mock_datetime(2017, 9, 4, 16, 1):
            default_cron.run_pending()
            assert mock_task.call_count == 4
        with mock_datetime(2017, 9, 4, 17, 1):
            default_cron.run_pending()
            assert mock_task.call_count == 4
        mock_task2 = make_mock_task()
        with mock_datetime(2017, 9, 4, 17, 1):
            default_cron.new_task().at_hours("12-16").do(mock_task2)
            default_cron.run_pending()
            assert mock_task2.call_count == 0

    def test_run_at_specified_hours_and_minutes(self):
        mock_task = make_mock_task()
        with mock_datetime(2017, 9, 4, 9, 0):
            default_cron.new_task().at_hours("12-14").at_minutes("15, 45").do(mock_task)
            default_cron.run_pending()
            assert mock_task.call_count == 0

        with mock_datetime(2017, 9, 4, 12, 16):
            default_cron.run_pending()
            assert mock_task.call_count == 1

        with mock_datetime(2017, 9, 4, 12, 46):
            default_cron.run_pending()
            assert mock_task.call_count == 2

        with mock_datetime(2017, 9, 4, 13, 14):
            default_cron.run_pending()
            assert mock_task.call_count == 2

        with mock_datetime(2017, 9, 4, 14, 16):
            default_cron.run_pending()
            assert mock_task.call_count == 3

    def test_run_on_specified_days_at_specified_times(self):
        mock_task = make_mock_task()
        with mock_datetime(2017, 9, 4, 9, 0):
            default_cron.new_task().on_days("2, 4").at_hours("12").at_minutes("15, 45").do(mock_task)
            default_cron.run_pending()
            assert mock_task.call_count == 0

        with mock_datetime(2017, 9, 5, 12, 16):
            default_cron.run_pending()
            assert mock_task.call_count == 0

        with mock_datetime(2017, 9, 6, 12, 14):
            default_cron.run_pending()
            assert mock_task.call_count == 0

        with mock_datetime(2017, 9, 6, 12, 16):
            default_cron.run_pending()
            assert mock_task.call_count == 1

        with mock_datetime(2017, 9, 6, 12, 46):
            default_cron.run_pending()
            assert mock_task.call_count == 2

        with mock_datetime(2017, 9, 6, 13, 16):
            default_cron.run_pending()
            assert mock_task.call_count == 2

    def test_configured(self):
        mock_task = make_mock_task()
        task = default_cron.new_task()
        assert task.configured is False
        task.every_day()
        assert task.configured is False
        task.at("9:00")
        assert task.configured is False
        task.do(mock_task)
        assert task.configured is True

    def test_tags(self):
        mock_task = make_mock_task()
        task = default_cron.new_task().every_day().at_hours("0:23").do(mock_task).tag("NewTag")
        assert "NewTag" in task.tags

    def test_run_tagged(self):
        mock_task_1 = make_mock_task()
        mock_task_2 = make_mock_task()
        default_cron.new_task().every_day().at_minutes("0:59").do(mock_task_1).tag("Fancy")
        default_cron.new_task().every_day().at_minutes("0:59").do(mock_task_1)

        default_cron.run_tagged("Fancy")
        assert mock_task_1.call_count == 1
        assert mock_task_2.call_count == 0


if __name__ == '__main__':
    unittest.main(verbosity=1)