# CronPy
[![Build Status](https://travis-ci.org/speedyturkey/cronpy.svg?branch=master)](https://travis-ci.org/speedyturkey/cronpy)
[![Coverage Status](https://coveralls.io/repos/github/speedyturkey/cronpy/badge.svg?branch=master)](https://coveralls.io/github/speedyturkey/cronpy?branch=master)

Features
* Flexible scheduling
* Convenience functions
* Easily run all jobs with a certain tag.

Sample Code

```python
def task():
    print("CronPy is working!")

cronpy.new_task().every_day().at("9:00").do(task)
cronpy.new_task().every_day().at_hours("0-23").do(task)
cronpy.new_task().on_days("0-4").at_hours("9-17").at_minutes("15, 45").do(task)

cronpy.run_continuously()
```