# -*- coding: UTF-8 -*-

import os
import sys
import json
import yaml
import logging

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from datetime import datetime
from logs import fatal_exception

# A cronjob specification
#   minute field: set(0..59)
#   hour field: set(0..59)
#   day field: set(1..31)
#   month field: set(1..12)
#   day-of-week field: set(0..6)
class CronJobSpec:
  def __init__(self, spec):
    field_strings = spec.split(' ')
    if len(field_strings) != 5:
      raise ValueError('invalid number of fields in cronjob specification')
    self.minute = CronJobFieldSpec(field_strings[0], 0, 59)
    self.hour = CronJobFieldSpec(field_strings[1], 0, 59)
    self.day = CronJobFieldSpec(field_strings[2], 1, 31)
    self.month = CronJobFieldSpec(field_strings[3], 1, 12)
    self.dow = CronJobFieldSpec(field_strings[4], 0, 6)

  def __repr__(self):
    return '%s %s %s %s %s' % (self.minute, self.hour, self.day, self.month, self.dow)

def check_cronjob_field_value(value, lowest, highest):
  try:
    if value == '*':
      return (lowest, highest)
    value = int(value)
    if lowest >= 0 and value < lowest:
      raise ValueError('invalid field value %d in cronjob specification' % (value))
    if highest >= 0 and value > highest:
      raise ValueError('invalid field value %d in cronjob specification' % (value))
    return (value, value)
  except ValueError as e:
    raise ValueError('invalid field value %s in cronjob specification' % (value))

class CronJobFieldSpec:
  def __init__(self, spec, lowest, highest):
    values = set()
    for entry in spec.split(','):
      splits = entry.split('/')
      if len(splits) == 1:
        rangeval = splits[0]
        every = 1
      elif len(splits) == 2:
        rangeval = splits[0]
        every = check_cronjob_field_value(splits[1], 1, highest)
      else:
        raise ValueError('invalid entry %s in cronjob specification: %s' % (entry, spec))
      boundaries = rangeval.split('-')
      if len(boundaries) == 1:
        (low, high) = check_cronjob_field_value(boundaries[0], lowest, highest)
      elif len(boundaries) == 2:
        low = check_cronjob_field_value(boundaries[0], lowest, highest)
        high = check_cronjob_field_value(boundaries[1], lowest, highest)
        if low > high:
          raise ValueError('invalid range values %d-%d in cronjob specification' % (low, high))
      else:
        raise ValueError('invalid field in cronjob specification: %s' % (spec))
      for value in range(low, high+1, every):
        values.add(value)
    self.values = values

  def __repr__(self):
    return ','.join(str(value) for value in sorted(self.values))

# A schedule rule entry
#   size field: a non-negative integer (0..n)
#   start field: a cronjob syntax string
#   stop field: a cronjob syntax string
class ScheduleRuleEntry:
  def __init__(self, size, start, end):
    self.size = size
    self.start = start
    self.end = end

  def __repr__(self):
    return 'size=%d start=%s end=%s' % (self.size, self.start, self.end)

# A schedule entry
#   name field: name string
#   entries field: a list of schedule rule entries
class ScheduleEntry:
  def __init__(self, entries):
    self.entries = entries

  def __repr__(self):
    return ','.join(str(entry) for entry in self.entries)

# Schedule entries
#   entries field: a dict of schedule entries
class Schedules:
  def __init__(self, entries):
    self.entries = entries

  def __repr__(self):
    return ','.join('%s=%s' % (key, str(value)) for key, value in self.entries.items())

# A node pool rule entry
#   schedule field: name of a schedule entry
#   compartment field: an optional compartment name or id filter string
#   cluster field: an optional cluster name filter or id string
#   nodepool field: an optional node pool name filter or id string
class RuleEntry:
  def __init__(self, schedule, compartment, cluster, nodepool):
    self.schedule = schedule
    self.compartment = compartment
    self.cluster = cluster
    self.nodepool = nodepool

  def __repr__(self):
    return 'schedule=%s compartment==%s cluster=%s nodepool=%s' % (self.schedule, self.compartment, self.cluster, self.nodepool)

# Node pool rules list
#   entries field: a list of node pool rule entries
class Rules:
  def __init__(self, entries):
    self.entries = entries

  def __repr__(self):
    return ','.join(str(entry) for entry in self.entries)

# An exception entry
#   start: an optional start date/time specification
#   end: an optional end date/time specification
#   compartment field: an optional compartment name or id filter string
#   cluster field: an optional cluster name filter or id string
#   nodepool field: an optional node pool name filter or id string
#   size field: a non-negative integer (0..n), or None (on)
#   comment field: an optional comment string
class ExceptionEntry:
  def __init__(self, start, end, compartment, cluster, nodepool, size, comment):
    self.start = start
    self.end = end
    self.compartment = compartment
    self.cluster = cluster
    self.nodepool = nodepool
    self.size = size
    self.comment = comment

  def __repr__(self):
    return 'start=%s end=%s compartment==%s cluster=%s nodepool=%s size=%s comment=%s' % (self.start, self.end, self.compartment, self.cluster, self.nodepool, self.size, self.comment)

  def process_entry(self, timezone):
    if self.start is not None:
      self.start = datetime(self.start.year, self.start.month, self.start.day, self.start.hour, self.start.minute, self.start.second, tzinfo=timezone)
    if self.end is not None:
      self.end = datetime(self.end.year, self.end.month, self.end.day, self.end.hour, self.end.minute, self.end.second, tzinfo=timezone)

# Excepions list
#   entries field: a list of exception entries
class Exceptions:
  def __init__(self, entries):
    self.entries = entries

  def __repr__(self):
    return ','.join(str(entry) for entry in self.entries)

  def process_entries(self, timezone):
    for entry in self.entries:
      entry.process_entry(timezone)

# example config:
# schedules:
#   everyday:
#     - start: "0 20 5 * *"
#       end: "0 6 1 * *"
#       size: 0
#     - start: "0 20 1 * *"
#       end: "0 6 2 * *"
#       size: 0
#     - start: "0 20 2 * *"
#       end: "0 6 3 * *"
#       size: 0
#     - start: "0 20 3 * *"
#       end: "0 6 4 * *"
#       size: 0
#     - start: "0 20 4 * *"
#       end: "0 6 5 * *"
#       size: 0
#   weekend:
#     - start: "0 20 5 * *"
#       end: "0 6 1 * *"
#       size: 0
#   none: {}
# rules:
#   - compartment: sandbox/devops
#     schedule: everyday
#   - compartment: enap/cmp-tst
#     schedule: everyday
#   - compartment: enap/cmp-uat
#     schedule: weekend
#   - compartment: enap/cmp-prod
#     schedule: none
# exceptions:
#   - comment: Weekend testing
#     compartment: sandbox/devops
#     start: 2025-12-19 18:00
#     end: 2025-12-22 06:00
#     size: on
#   - comment: Holiday
#     start: 2025-12-24 00:00
#     end: 2025-12-28 00:00
#     size: 0

class ConfigError(Exception):
  def __init__(self, message):
    super().__init__(message)

class Config:
  DEFAULT_CONFIG_FILE = "rules.yaml"

  config = None

# initialize global config instance
  def read_config():
    return Config()

  def dump(self):
    return "timezone=%s\nschedules=%s\nrules=%s\nexceptions=%s" % (self.timezone, self.schedules, self.rules, self.exceptions)

# read config file and init object
  def __init__(self):
    try:
      config_filename = os.getenv('CONFIG_FILE', Config.DEFAULT_CONFIG_FILE)
      logging.info('Reading config file %s' % (config_filename))
      stream = open(config_filename, 'r')
      self.config = yaml.safe_load(stream)
      self.parse_config_contents()
    except ConfigError as e:
      logging.error(e.args[0])
      sys.exit(1)
    except Exception as e:
      fatal_exception('during loading config file', e)

# parse config contents
  def parse_config_contents(self):
    self.timezone = self.check_config_option_timezone(None, 'timezone', None)
    self.schedules = self.check_config_option_schedules('schedules')
    self.rules = self.check_config_option_rules('rules')
    self.exceptions = self.check_config_option_exceptions('exceptions')
    self.exceptions.process_entries(self.timezone)

# check config option
  def check_config_global_option(self, name, default=None, skipEmpty=False):
    name_components = name.split('.')
    env_name = '_'.join(name_components).upper()
    value = os.getenv(env_name)
    if value is not None:
      if value != '':
        return value
      if not skipEmpty:
        return value
    ptr = self.config
    found = True
    for component in name_components:
      if not isinstance(ptr, dict):
        found = False
        break
      if not component in ptr:
        found = False
        break
      ptr = ptr[component]
    if found:
      return ptr
    return default

# check integer config option
  def check_config_option_integer(self, base, name, key, min=-1, max=-1, default=None, emptyDefault=False, skipEmpty=False):
    if base is None:
      value = self.check_config_global_option(name, default, skipEmpty=skipEmpty)
    else:
      name = '%s.%s' % (name, key)
      value = base[key] if key in base else None
    if value is None:
      if min > 0:
        raise ConfigError('Invalid config option %s: missing but mandatory' % (name))
      return value
    if isinstance(value, (int, float)):
      return int(value)
    if isinstance(value, list):
      raise ConfigError('Invalid config option %s type: list' % (name))
    if isinstance(value, dict):
      raise ConfigError('Invalid config option %s type: dict' % (name))
    value = str(value)
    if value == '' and emptyDefault:
      return default
    try:
      value = int(value)
      if min >= 0 and value < min:
        raise ConfigError('Invalid config option %s: \'%s\' is smaller than minimum %d' % (name, value, min))
      if max >= 0 and value > max:
        raise ConfigError('Invalid config option %s: \'%s\' is larger than maximum %d' % (name, value, max))
      return value
    except ValueError as e:
      raise ConfigError('Invalid config option %s: \'%s\'' % (name, value))

# check string config option
  def check_config_option_string(self, base, name, key, minLen=-1, maxLen=-1, default=None, emptyDefault=False, skipEmpty=False):
    if base is None:
      value = self.check_config_global_option(name, default, skipEmpty=skipEmpty)
    else:
      name = '%s.%s' % (name, key)
      value = base[key] if key in base else None
    if value is None:
      if minLen > 0:
        raise ConfigError('Invalid config option %s: missing but mandatory' % (name))
      return value
    if isinstance(value, list):
      raise ConfigError('Invalid config option %s type: list' % (name))
    if isinstance(value, dict):
      raise ConfigError('Invalid config option %s type: dict' % (name))
    value = str(value)
    length = len(value)
    if value == '' and emptyDefault:
      return default
    if minLen >= 0 and length < minLen:
      raise ConfigError('Invalid config option %s: \'%s\' is shorter than minimum %d' % (name, value, minLen))
    if maxLen >= 0 and length > maxLen:
      raise ConfigError('Invalid config option %s: \'%s\' is longer than maximum %d' % (name, value, maxLen))
    return value

# check date/time config options
  def check_config_option_datetime(self, base, name, key):
    datetimeval = self.check_config_option_string(base, name, key, minLen=0)
    if datetimeval is None or datetimeval == '':
      return None
    return datetime.fromisoformat(datetimeval)

# check cronjob config option
  def check_config_option_cronjob(self, base, name, key):
    return CronJobSpec(self.check_config_option_string(base, name, key, minLen=1))

# check list or string config option
  def check_config_option_list_or_string(self, name, minNum=-1, maxNum=-1, default=None, skipEmpty=False):
    value = self.check_config_global_option(name, default, skipEmpty=skipEmpty)
    if value is None:
      if minNum > 0:
        raise ConfigError('Invalid config option %s: missing but mandatory' % (name))
    if isinstance(value, dict):
      raise ConfigError('Invalid config option %s type: dict' % (name))
    if isinstance(value, list):
      listvalue = value
      retlist = []
      if len(listvalue) < 1 and emptyDefault:
        return default
      for value in listvalue:
        if isinstance(value, list):
          raise ConfigError('Invalid config option %s list entry type: list' % (name))
        if isinstance(value, dict):
          raise ConfigError('Invalid config option %s list entry type: dict' % (name))
        value = str(value)
        retlist.append(value)
      if minNum > 0 and minNum > len(retlist):
        raise ConfigError('Invalid config option %s: list length %d is less than %d values expected' % (name, len(retlist), minNum))
      if maxNum > 0 and maxNum < len(retlist):
        raise ConfigError('Invalid config option %s: list length %d is more than %d values allowed' % (name, len(retlist), maxNum))
      return retlist
    value = str(value)
    length = len(value)
    if value == '' and emptyDefault:
      return default
    if minNum > 1:
      raise ConfigError('Invalid config option %s: a single value \'%s\' is less than %d values expected' % (name, value, minNum))
    return [value]

# check dict config option
  def check_config_option_dict(self, name, minNum=-1, maxNum=-1, default=None, skipEmpty=False):
    value = self.check_config_global_option(name, default, skipEmpty=skipEmpty)
    if value is None:
      if minNum > 0:
        raise ConfigError('Invalid config option %s: missing but mandatory' % (name))
    if not isinstance(value, dict):
      raise ConfigError('Invalid config option %s type: not a dict' % (name))
    dictvalue = value
    retdict = {}
    if len(dictvalue) < 1 and emptyDefault:
      return default
    for key, value in dictvalue.items():
      if isinstance(value, list):
        raise ConfigError('Invalid config option %s dict entry type: list' % (name))
      if isinstance(value, dict):
        raise ConfigError('Invalid config option %s dict entry type: dict' % (name))
      value = str(value)
      retdict[key] = value
    if minNum > 0 and minNum > len(retdict):
      raise ConfigError('Invalid config option %s: dict size %d is less than %d values expected' % (name, len(retdict), minNum))
    if maxNum > 0 and maxNum < len(retdict):
      raise ConfigError('Invalid config option %s: dict size %d is more than %d values allowed' % (name, len(retdict), maxNum))
    return retdict

# check 'exceptions' entry
  def check_config_exception_entry(self, name, value):
    if isinstance(value, list):
      raise ConfigError('Invalid config option %s type: list' % (name))
    if not isinstance(value, dict):
      raise ConfigError('Invalid config option %s type: not a dict' % (name))
    start = self.check_config_option_datetime(value, name, 'start')
    end = self.check_config_option_datetime(value, name, 'end')
    compartment = self.check_config_option_string(value, name, 'compartment', minLen=0)
    cluster = self.check_config_option_string(value, name, 'cluster', minLen=0)
    nodepool = self.check_config_option_string(value, name, 'nodepool', minLen=0)
    size = self.check_config_option_integer(value, name, 'size', min=0, emptyDefault=True)
    comment = self.check_config_option_string(value, name, 'comment', minLen=0)
    return ExceptionEntry(start, end, compartment, cluster, nodepool, size, comment)

# check 'exceptions' config option
  def check_config_option_exceptions(self, name):
    value = self.check_config_global_option(name, [], False)
    exceptions = []
    if value is not None:
      if isinstance(value, dict):
        raise ConfigError('Invalid config option %s type: dict' % (name))
      if not isinstance(value, list):
        raise ConfigError('Invalid config option %s type: not a list' % (name))
      for i, entry in enumerate(value, start=1):
        if isinstance(entry, list):
          raise ConfigError('Invalid config option %s list entry type: list' % (name))
        if not isinstance(entry, dict):
          raise ConfigError('Invalid config option %s list entry type: not a dict' % (name))
        exceptions.append(self.check_config_exception_entry('%s[%d]' % (name, i), entry))
    return Exceptions(exceptions)

# check 'rules' entry
  def check_config_rule_entry(self, name, value):
    if isinstance(value, list):
      raise ConfigError('Invalid config option %s type: list' % (name))
    if not isinstance(value, dict):
      raise ConfigError('Invalid config option %s type: not a dict' % (name))
    schedule = self.check_config_option_string(value, name, 'schedule', minLen=1)
    compartment = self.check_config_option_string(value, name, 'compartment', minLen=0)
    cluster = self.check_config_option_string(value, name, 'cluster', minLen=0)
    nodepool = self.check_config_option_string(value, name, 'nodepool', minLen=0)
    return RuleEntry(schedule, compartment, cluster, nodepool)

# check 'rules' config option
  def check_config_option_rules(self, name):
    value = self.check_config_global_option(name, [], False)
    rules = []
    if value is not None:
      if isinstance(value, dict):
        raise ConfigError('Invalid config option %s type: dict' % (name))
      if not isinstance(value, list):
        raise ConfigError('Invalid config option %s type: not a list' % (name))
      for i, entry in enumerate(value, start=1):
        if isinstance(entry, list):
          raise ConfigError('Invalid config option %s list entry type: list' % (name))
        if not isinstance(entry, dict):
          raise ConfigError('Invalid config option %s list entry type: not a dict' % (name))
        rules.append(self.check_config_rule_entry('%s[%d]' % (name, i), entry))
    return Rules(rules)

# check 'schedules' rule entry
  def check_config_schedule_rule_entry(self, name, value):
    if isinstance(value, list):
      raise ConfigError('Invalid config option %s type: list' % (name))
    if not isinstance(value, dict):
      raise ConfigError('Invalid config option %s type: not a dict' % (name))
    size = self.check_config_option_integer(value, name, 'size', min=0)
    startcron = self.check_config_option_cronjob(value, name, 'start')
    endcron = self.check_config_option_cronjob(value, name, 'end')
    return ScheduleRuleEntry(size, startcron, endcron)

# check 'schedules' entry
  def check_config_schedule_entry(self, name, value):
    if isinstance(value, dict):
      raise ConfigError('Invalid config option %s dict entry type: dict' % (name))
    entries = []
    if value is not None:
      if not isinstance(value, list):
        raise ConfigError('Invalid config option %s dict entry type: not a list' % (name))
      for i, entry in enumerate(value, start=1):
        entries.append(self.check_config_schedule_rule_entry('%s[%d]' % (name, i), entry))
    return ScheduleEntry(entries)

# check 'schedules' config option
  def check_config_option_schedules(self, name):
    value = self.check_config_global_option(name, [], False)
    schedules = {}
    if value is not None:
      if isinstance(value, list):
        raise ConfigError('Invalid config option %s type: list' % (name))
      if not isinstance(value, dict):
        raise ConfigError('Invalid config option %s type: not a dict' % (name))
      dictvalue = value
      for (key, value) in dictvalue.items():
        schedules[key] = self.check_config_schedule_entry('%s.%s' % (name, key), value)
    return Schedules(schedules)

# check timezone config option
  def check_config_option_timezone(self, base, name, key):
    timezonename = self.check_config_option_string(base, name, key, minLen=1)
    try:
      return ZoneInfo(timezonename)
    except ZoneInfoNotFoundError as e:
      raise ConfigError('Invalid timezone %s: %s' % (timezonename, e.args[0]))
