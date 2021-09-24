# fauxjo
A database persistence layer for the real world.

11.3.0-13   Made ResultSetIterator AutoCloseable so that it could be used in the new-style 
            an auto-closable try block.

11.3.0-12   Made Home and Table AutoCloseable so that it could be used in the new-style 
            an auto-closable try block.

11.3.0-11   Made HomeGroup AutoCloseable so that it could be used in the new-style an
            auto-closable try block.

11.3.0-10   Fixed bug in Table.update that may occur when the database has an extra column
            that is not in the fauxjo java bean class. The error would be
            "Unable to find FieldDef [fieldName]" during an update.

11.3.0-9    Removed StatementCache and Connection from Home and re-routed their calls
            to new methods in its Table. Added methods to return StatementCache stats.
            StatementCacheListener param API break from StatementCache.CacheType enum
            to StatementCache.Config object.

11.3.0-8    Enabled StatementCache by default. Try-finally close PreparedStatements 
            in Table methods only if StatementCache is disabled. Other minor fixes 
            and improved javadoc.

11.3.0-7    Added a listener for and updated StatementCache to work with HikariCP 
            including rapid recovery support and configuration comments. Added ability to 
            disable it via its Home and related comments. Also added a configurable LRU to 
            it to avoid memory leaks from PreparedStatement/CallableStatement sql that 
            incorrectly concatenates criteria instead of uses parameters.

