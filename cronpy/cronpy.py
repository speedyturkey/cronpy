import functools
import datetime
import re
import itertools
from time import sleep


class Cron(object):
    """
    Key methods
    new_task - create a task and register it to run on the Cron instance.
    run_all
    run_with_tag - run all tasks with specified tag.
    """

    def __init__(self):
        self.tasks = []

    def __repr__(self):
        return "<Cron> object containing {} tasks".format(len(self.tasks))

    def new_task(self):
        return Task(cron=self)

    def run_all(self):
        pass

    def run_pending(self):
        """
        Execute any task which is currently scheduled to run.
        """
        tasks_to_run = (task for task in self.tasks if task.should_run)
        for task in sorted(tasks_to_run):
            task.run()

    def run_continuously(self):
        """
        Execute run_pending method every 10 seconds on a continuous basis.
        """
        while True:
            self.run_pending()
            sleep(10)

    def run_tagged(self, tag):
        """
        :param tag:
        Executes all tasks with the provided tag, whether currently scheduled or not.
        """
        tagged_tasks = (task for task in self.tasks if tag in task.tags)
        for task in tagged_tasks:
            task.run()


class Task(object):
    """
    Represents a single function/job/task to be executed on a regular basis.
    Can be set to to execute on specified days, at specific hours and minutes.
    Each <Task> object is registered to a <Cron> object, which controls actual execution.
    """

    DAY_NAMES = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    DAYS = [0, 1, 2, 3, 4, 5, 6]

    def __init__(self, cron):

        self.cron = cron    # Associate task with provided Cron instance.

        self.days = []      # Days on which to run.
        self.hours = []     # Hours during which to run.
        self.minutes = []   # Minutes past the hour at which to run.
        self.run_at = []    # Scheduled times to run on scheduled days.

        self.task_func = None
        self.last_run_at = None
        self.next_run_date = None
        self.next_run_time = None

        self.tags = set()  # unique set of tags for the job
        self.configured = False

    def __lt__(self, other_task):
        """
        Tasks are sortable based on the scheduled time they
        run next.
        """
        return self.next_run_at < other_task.next_run_at

    def __repr__(self):
        if self.configured:
            try:
                task_name = self.task_func.__name__
            except AttributeError:
                task_name = repr(self.task_func)
            next_run = self.next_run_at if self.next_run_at else "(not scheduled)"
            ret = "<Task> Do {}. Next run at {}".format(task_name, next_run)
        else:
            ret = "(Unconfigured <Task> object)"
        return ret

    @property
    def next_run_at(self):
        return datetime.datetime.combine(self.next_run_date, self.next_run_time)

    @property
    def should_run(self):
        return datetime.datetime.now() >= self.next_run_at

    def run(self):
        """
        :return: task_func return value
        Executes task_func and schedules next execution.
        """
        ret = self.task_func()
        self.last_run_at = datetime.datetime.now()
        self.schedule_next_run()
        return ret

    def tag(self, *tags):
        """
        :return: self
        Adds unique tags to this instance of <Task>
        """
        self.tags.update(tags)
        return self

    def do(self, task_func, *args, **kwargs):
        """
        :param task_func: Function object to be executed by this task.
        :param args: Ordered arguments to be provided to task_func (optional).
        :param kwargs: Keyword arguments to be provided to task_func (optional).
        :return: self

        Creates a function partial to be executed on a scheduled basis as defined by this
        task's configuration, schedules the next execution time, and registers the task
        to its associated instance of <Cron>.
        """
        self.task_func = functools.partial(task_func, *args, **kwargs)
        try:
            functools.update_wrapper(self.task_func, task_func)
        except AttributeError:
            pass
        if self.hours and not self.minutes:
            # Run on the hour if not specified otherwise.
            self.minutes = [0]
        if self.run_at and self.hours and self.minutes:
            raise ValueError("Cannot specify both `run_at` time and specific hours/minutes.")
        if self.hours and self.minutes:
            # Create list of time objects representing all valid combinations of hours and minutes.
            self.run_at = [datetime.time(p[0], p[1]) for p in itertools.product(self.hours, self.minutes)]
        self.schedule_next_run()
        self.cron.tasks.append(self)
        self.configured = True
        return self

    def every_day(self):
        """Execute this task each day of the week."""
        self.days = Task.DAYS
        return self

    def on_days(self, day_range):
        """
        :param day_range: Range of days as string in format "0-4" or "0:4", or list of days in format "0, 2, 4"
        :return: self

        Sets specific days of the week on which to execute this task.
        """
        delimiters = set(re.findall("[:,-]", day_range))
        if len(delimiters) > 1:
            raise ValueError("List must contain only one delimiter.")
        day_list = re.split("[:,-]", day_range)
        day_list = [int(i) for i in day_list]
        if set(day_list) - set(Task.DAYS):
            raise ValueError("List must contain only valid days.")
        if len(day_list) > 2 and "," not in delimiters:
            raise ValueError("Must provide exactly two days for range but provided {}.".format(len(day_list)))
        if "," in delimiters:
            self.days = day_list
        elif len(day_list) > 1:
            if day_list[0] > day_list[1]:
                raise ValueError("Starting day must be before ending day.")
            start_day, end_day = day_list
            self.days = Task.DAYS[slice(start_day, end_day + 1)]
        else:
            self.days = day_list
        return self

    def at_hours(self, hour_range):
        """
        :param hour_range: Range of hours as string in format "0-4" or "0:4", or list of hours in format "0, 2, 4"
        :return: self

        Sets specific hours of the day during which to execute this task.
        """
        if not self.days:
            self.every_day()
        delimiters = set(re.findall("[:,-]", hour_range))
        if len(delimiters) > 1:
            raise ValueError("List must contain only one delimiter.")
        hour_list = re.split("[:,-]", hour_range)
        hour_list = [int(i) for i in hour_list]
        assert min(hour_list) >= 0
        assert max(hour_list) <= 23
        if len(hour_list) > 2 and "," not in delimiters:
            raise ValueError("Must provide exactly two hours for range but provided {}.".format(len(hour_list)))
        if "," in delimiters:
            self.hours = hour_list
        elif len(hour_list) > 1:
            if hour_list[0] > hour_list[1]:
                raise ValueError("Starting hour must be before ending hour.")
            start_hour, end_hour = hour_list
            self.hours = list(range(start_hour, end_hour + 1))
        else:
            self.hours = hour_list
        return self

    def at_minutes(self, minute_range):
        """
        :param minute_range: Range of minutes as string in format "0-4" or "0:4", or list of minutes in format "0, 15, 30"
        :return: self

        Sets specific minutes of the hour during which to execute this task.
        """
        if not self.days:
            self.every_day()
        if not self.hours:
            self.at_hours("0-23")
        delimiters = set(re.findall("[:,-]", minute_range))
        if len(delimiters) > 1:
            raise ValueError("List must contain only one delimiter.")
        minute_list = re.split("[:,-]", minute_range)
        minute_list = [int(i) for i in minute_list]
        assert min(minute_list) >= 0
        assert max(minute_list) <= 59
        if len(minute_list) > 2 and "," not in delimiters:
            raise ValueError("Must provide exactly two minutes for range but provided {}.".format(len(minute_list)))
        if "," in delimiters:
            self.minutes = minute_list
        elif len(minute_list) > 1:
            if minute_list[0] > minute_list[1]:
                raise ValueError("Starting minute must be before ending minute.")
            start_minute, end_minute = minute_list
            self.minutes = list(range(start_minute, end_minute + 1))
        else:
            self.minutes = minute_list
        return self

    def hour_interval(self, interval):
        """
        :param interval: Integer between 1 and 24. 24 hours will be split into intervals of size `interval`.
        :return self
        This <Task> will be executed only during the hours specified by the interval. For example, if `interval` == 6,
        execution will take place during the hours 00:00, 06:00, 12:00, and 18:00.
        """
        assert 1 <= interval <= 24
        self.hours = list(range(0, 24))[::interval]
        return self

    def minute_interval(self, interval):
        """
        :param interval: Integer between 1 and 60. 60 minutes will be split into intervals of size `interval`.
        :return self
        This <Task> will be executed only at the minutes specified by the interval. For example, if `interval` == 15,
        execution will take place at 0, 15, 30 and 45 minutes past the hour.
        """
        assert 1 <= interval <= 60
        self.minutes = list(range(0, 60))[::interval]
        return self

    def monday(self):
        """Schedule task to run on Mondays"""
        self.days.append(0)
        return self

    def tuesday(self):
        """Schedule task to run on Tuesdays"""
        self.days.append(1)
        return self

    def wednesday(self):
        """Schedule task to run on Wednesdays"""
        self.days.append(2)
        return self

    def thursday(self):
        """Schedule task to run on Thursdays"""
        self.days.append(3)
        return self

    def friday(self):
        """Schedule task to run on Fridays"""
        self.days.append(4)
        return self

    def saturday(self):
        """Schedule task to run on Saturdays"""
        self.days.append(5)
        return self

    def sunday(self):
        """Schedule task to run on Sundays"""
        self.days.append(6)
        return self

    def at(self, time):
        """
        :param time: Time as string (e.g. "09:30", "14:45")
        :return: self

        Set time of day to run task. If days have not already been set,
        will set task to run every day. This method can be used to set
        an arbitrary number of times to run each day.
        """
        if not self.days:
            self.every_day()
        hour, minute = [int(i) for i in time.split(":")]
        assert 0 <= hour <= 23
        assert 0 <= minute <= 59
        self.run_at.append(datetime.time(hour, minute))
        return self

    def schedule_next_run(self):
        """
        Calculate date and time of next scheduled task execution.
        """
        self.calculate_next_run_time()
        self.calculate_next_run_date()
        if self.next_run_at < datetime.datetime.now():
            raise Exception("Scheduled at {}, but it is {} now.".format(self.next_run_at, datetime.datetime.now()))

    def calculate_next_run_date(self):
        next_run_date = datetime.datetime.today().date()
        if self.days:
            while next_run_date.weekday() not in self.days:
                next_run_date += datetime.timedelta(days=1)
            if self.run_at:
                if datetime.datetime.now().time() > max(self.run_at):
                    next_run_date += datetime.timedelta(days=1)
                while next_run_date.weekday() not in self.days:
                    next_run_date += datetime.timedelta(days=1)

        self.next_run_date = next_run_date

    def calculate_next_run_time(self):
        times_later_today = [time for time in self.run_at if time >= datetime.datetime.now().time()]
        if times_later_today:
            self.next_run_time = min(times_later_today)
        else:
            self.next_run_time = min(self.run_at)


# Default instance of <Cron>
default_cron = Cron()


def new_task():
    """
    :return: Return an instance of <Task> registered to the default <Cron> instance
    """
    return default_cron.new_task()


def run_pending():
    """
    Execute the run_pending method of the default <Cron> instance
    """
    default_cron.run_pending()


