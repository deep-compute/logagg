#!/usr/bin/env python

import doctest
import unittest

from logagg import formatters
from logagg import util
from logagg import collector
from logagg import forwarders

def suite_maker():
    suite= unittest.TestSuite()
    suite.addTests(doctest.DocTestSuite(formatters))

    suite.addTests(doctest.DocTestSuite(collector))

    suite.addTests(doctest.DocTestSuite(forwarders))

    suite.addTests(doctest.DocTestSuite(util))
    return suite

