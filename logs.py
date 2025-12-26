# -*- coding: UTF-8 -*-

import sys
import logging
import traceback

# print fatal exception and exit
def fatal_exception(msg, e, code = 2):
  prefix = ' '
  if msg is None or msg == '':
    prefix = ''
    msg = ''
  sys.stdout.write("Exception caught%s%s: %s: %s\n" % (prefix, msg, type(e).__name__, e))
  traceback.print_tb(e.__traceback__)
  sys.exit(code)
