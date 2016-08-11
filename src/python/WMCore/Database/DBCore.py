#!/usr/bin/env python
"""
_Core_

Core Database APIs


"""
import traceback
from WMCore.DataStructs.WMObject import WMObject
from WMCore.Database.ResultSet import ResultSet
from copy import copy
import WMCore.WMLogging

from sqlalchemy.dialects.oracle.cx_oracle import OracleDialect_cx_oracle

# OracleHandler is required to convert input unicode values
# into byte strings since Oracle DB expects them
# The issue was discovered in https://github.com/dmwm/WMCore/pull/6896
# and elegant solution found in sam-web
# https://cdcvs.fnal.gov/redmine/projects/sam-web/repository/revisions/oracle/entry/python/samdb/database.py
def OracleInputTypeHandler(cursor, value, numElements):
    """ Convert unicode objects to byte strings before sending them to Oracle """
    if isinstance(value, unicode):
        encoding = cursor.connection.encoding
    # the lambda below must not capture the cursor as it creates a circular reference
    return cursor.var(str, arraysize = numElements, inconverter = lambda x: x.encode(encoding))

class DBInterface(WMObject):
    """
    Base class for doing SQL operations using a SQLAlchemy engine, or
    pre-exisitng connection.

    processData will take a (list of) sql statements and a (list of)
    bind variable dictionaries and run the statements on the DB. If
    necessary it will substitute binds into the sql (MySQL).

    TODO:
        Add in some suitable exceptions in one or two places
        Test the hell out of it
        Support executemany()
    """

    logger = None
    engine = None

    def __init__(self, logger, engine):
        super(DBInterface, self).__init__(self)
        self.logger = logger
        self.logger.info ("Instantiating base WM DBInterface")
        self.engine = engine
        self.maxBindsPerQuery = 500

    def buildbinds(self, sequence, thename, therest=None):
        """
        Build a list of binds. Can be used recursively, e.g.:
        buildbinds(file, 'file', buildbinds(pnn, 'location'), {'lumi':123})
        TODO: replace with an appropriate map function
        """
        if  not therest:
            therest = [{}]
        binds = []
        for r in sequence:
            for i in self.makelist(therest):
                thebind = copy(i)
                thebind[thename] = r
                binds.append(thebind)
        return binds

    def executebinds(self, s=None, b=None, connection=None,
                     returnCursor=False):
        """
        _executebinds_

        returns a list of sqlalchemy.engine.base.ResultProxy objects
        """
        try:
            if b == None:
                resultProxy = connection.execute(s)
            else:
                resultProxy = connection.execute(s, b)
        except:
            traceback.print_exc()
            resultProxy = None
        if resultProxy:
            yield resultProxy
        if not returnCursor:
            if resultProxy and hasattr(resultProxy, "close"):
                resultProxy.close()

    def executemanybinds(self, s=None, b=None, connection=None,
                         returnCursor=False):
        """
        _executemanybinds_
        b is a list of dictionaries for the binds, e.g.:

        b = [ {'bind1':'value1a', 'bind2': 'value2a'},
        {'bind1':'value1b', 'bind2': 'value2b'} ]

        see: http://www.gingerandjohn.com/archives/2004/02/26/cx_oracle-executemany-example/

        Can't executemany() selects - so do each combination of binds here instead.
        This will return a list of sqlalchemy.engine.base.ResultProxy object's
        one for each set of binds.

        returns a list of sqlalchemy.engine.base.ResultProxy objects
        """

        s = s.strip()
        if s.lower().endswith('select', 0, 6):
            # Trying to select many
            if returnCursor:
                for bind in b:
                    result = connection.execute(s, bind)
                    yield result
            else:
                for bind in b:
                    resultproxy = connection.execute(s, bind)
                    yield resultproxy
                    resultproxy.close()
            return
        try:
            result = connection.execute(s, b)
        except:
            traceback.print_exc()
            result = None
        if result:
            yield result

    def connection(self):
        """
        Return a connection to the engine (from the connection pool)
        """
        return self.engine.connect()

    def processData(self, sqlstmt, binds=None, conn=None,
                    transaction=True, returnCursor=False):
        """Wrapper around _processData generator"""
        if  not binds:
            binds = {}
        connection = conn if conn else self.connection()

        gen = self._processData(sqlstmt, binds, connection, transaction, returnCursor)
        rset = ResultSet()
        for resultproxy in gen:
            rset.add(resultproxy)
        if not conn:
            connection.close()
        return [rset]

    def _processData(self, sqlstmt, binds=None, connection=None,
                    transaction=True, returnCursor=False):
        """
        set conn if you already have an active connection to reuse
        set transaction = True if you already have an active transaction

        """
        if  not binds:
            binds = {}
        # Can take either a single statement or a list of statements and binds
        sqlstmt = self.makelist(sqlstmt)
        binds = self.makelist(binds)
        if len(sqlstmt) > 0 and (len(binds) == 0 or (binds[0] == {} or binds[0] == None)):
            if transaction:
                trans = connection.begin()

            for i in sqlstmt:
                gen = self.executebinds(i, connection=connection,
                                      returnCursor=returnCursor)
                for rec in gen:
                    yield rec

            if transaction:
                trans.commit()
        elif len(binds) > len(sqlstmt) and len(sqlstmt) == 1:
            #Run single SQL statement for a list of binds - use execute_many()
            if transaction:
                trans = connection.begin()
            while(len(binds) > self.maxBindsPerQuery):
                gen = self._processData(sqlstmt, binds[:self.maxBindsPerQuery],
                                               connection=connection, transaction=True,
                                               returnCursor=returnCursor)
                for rec in gen:
                    yield rec
                binds = binds[self.maxBindsPerQuery:]

            for i in sqlstmt:
                gen = self.executemanybinds(i, binds, connection=connection,
                                                    returnCursor=returnCursor)
                for rec in gen:
                    yield rec

            if transaction:
                trans.commit()
        elif len(binds) == len(sqlstmt):
            # Run a list of SQL for a list of binds
            if transaction:
                trans = connection.begin()

            for i, s in enumerate(sqlstmt):
                b = binds[i]

                gen = self.executebinds(s, b, connection=connection,
                                      returnCursor=returnCursor)
                for rec in gen:
                    yield rec

            if transaction:
                trans.commit()
        else:
            self.logger.exception(
                "DBInterface.processData Nothing executed, problem with your arguments")
            self.logger.exception(
                "DBInterface.processData SQL = %s" % sqlstmt)
            WMCore.WMLogging.sqldebug('DBInterface.processData  sql is %s items long' % len(sqlstmt))
            WMCore.WMLogging.sqldebug('DBInterface.processData  binds are %s items long' % len(binds))
            assert_value = False
            if len(binds) == len(sqlstmt):
                assert_value = True
            WMCore.WMLogging.sqldebug('DBInterface.processData are binds and sql same length? : %s' % (assert_value))
            WMCore.WMLogging.sqldebug('sql: %s\n binds: %s\n, connection:%s\n, transaction:%s\n' %
                                       (sqlstmt, binds, connection, transaction))
            WMCore.WMLogging.sqldebug('type check:\nsql: %s\n binds: %s\n, connection:%s\n, transaction:%s\n' %
                                       (type(sqlstmt), type(binds), type(connection), type(transaction)))
            raise Exception("""DBInterface.processData Nothing executed, problem with your arguments
            Probably mismatched sizes for sql (%i) and binds (%i)""" % (len(sqlstmt), len(binds)))