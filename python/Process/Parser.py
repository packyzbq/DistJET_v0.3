import re
import types
import os

class Parser:
    fatalPattern = [
        # C
        '.*Segmentation fault',
        # SNiPER
        '.*ERROR:',
        '.*FATAL:',
        # Python
        '.*IOError',
        '.*ImportError',
        '.*TypeError',
        '.*MemoryError',
        '.*SyntaxError',
        '.*NameError',
        '.*RuntimeError',
        # Other
        '.*\*\*\* Break \*\*\* segmentation violation',
        '.*Warning in <TApplication::GetOptions>: macro .* not found',
    ]

    successPattern = []

    def __init__(self,cfg=None):
        self.fatal = set(Parser.fatalPattern)
        self.success = set(Parser.successPattern)
        if cfg:
            if cfg.getAttr['FatalPattern']:
                self.fatal.union(cfg.getAttr['FatalPattern'])
            if cfg.getAttr['SuccPattern']:
                self.fatal.union(cfg.getAttr['SuccPattern'])

    def addFatalPattern(self, pattern):
        if type(pattern) == types.ListType:
            self.fatal.union(pattern)
        elif type(pattern) == types.StringType:
            self.fatal.add(pattern)
        else:
            print('[Parser]: Invaid pattern type')

    def addSuccPattern(self,pattern):
        if type(pattern) == types.ListType:
            self.success.union(pattern)
        elif type(pattern) == types.StringType:
            self.success.add(pattern)
        else:
            print('[Parser]: Invaid pattern type')


    def parse(self, word):
        if not word:
            return True, None
        wordl = word.split('\n')
        for p in self.fatal:
            pattern = re.compile(p)
            for w in wordl:
                match = pattern.match(word)
                if match:
                    return False, "Fatal line: %s"%w
        return True,None

    def listPattern(self):
        print('Fatal Pattern: %s'%self.fatal)
        print('Success Pattern: %s'%self.success)
