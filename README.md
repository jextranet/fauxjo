# fauxjo
A database persistence layer for the real world.

11.3.0-8 Enabled StatementCache by default. Try-finally close PreparedStatements 
         in Table methods only if StatementCache is disabled. Other minor fixes 
         and improved javadoc.

11.3.0-7 Added a listener for and updated StatementCache to work with HikariCP 
         including rapid recovery support and configuration comments. Added ability to 
         disable it via its Home and related comments. Also added a configurable LRU to 
         it to avoid memory leaks from PreparedStatement/CallableStatement sql that 
         incorrectly concatenates criteria instead of uses parameters.

