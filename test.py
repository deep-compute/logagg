#!/usr/bin/env python

import doctest
import unittest

from logagg import *
def suite_maker():
    suite= unittest.TestSuite()
    suite.addTests(doctest.DocTestSuite(handlers))
    return suite
if __name__ == "__main__":
    doctest.testmod(handlers)
