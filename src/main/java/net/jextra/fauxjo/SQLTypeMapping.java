/*
 * Copyright (C) fauxjo.net.
 *
 * This file is part of the Fauxjo Library.
 *
 * The Fauxjo Library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * The Fauxjo Library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with the Fauxjo Library; if not, write to the Free
 * Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307 USA.
 */

package net.jextra.fauxjo;

import java.sql.*;
import java.util.*;

/**
 * Mapping between all SQL types and a Java type. It is intended to be used one-directional from SQL type -&gt; Java type.
 */
public class SQLTypeMapping extends HashMap<Integer,Class<?>> {
    // ============================================================
    // Fields
    // ============================================================

    private static SQLTypeMapping instance;

    // ============================================================
    // Constructors
    // ============================================================

    private SQLTypeMapping() {
        put(java.sql.Types.BOOLEAN, Boolean.class);
        put(java.sql.Types.BIT, Boolean.class);
        put(java.sql.Types.SMALLINT, Short.class);
        put(java.sql.Types.TINYINT, Short.class);
        put(java.sql.Types.INTEGER, Integer.class);
        put(java.sql.Types.BIGINT, Long.class);
        put(java.sql.Types.FLOAT, Float.class);
        put(java.sql.Types.DECIMAL, Double.class);
        put(java.sql.Types.DOUBLE, Double.class);
        put(java.sql.Types.NUMERIC, Double.class);
        put(java.sql.Types.REAL, Double.class);
        put(java.sql.Types.NVARCHAR, String.class);
        put(java.sql.Types.CHAR, String.class);
        put(java.sql.Types.CLOB, String.class);
        put(java.sql.Types.BINARY, String.class);
        put(java.sql.Types.LONGNVARCHAR, String.class);
        put(java.sql.Types.LONGVARBINARY, String.class);
        put(java.sql.Types.LONGVARCHAR, String.class);
        put(java.sql.Types.NCHAR, String.class);
        put(java.sql.Types.NCLOB, String.class);
        put(java.sql.Types.SQLXML, String.class);
        put(java.sql.Types.VARBINARY, String.class);
        put(java.sql.Types.VARCHAR, String.class);
        put(java.sql.Types.TIME, Time.class);
        put(java.sql.Types.TIME_WITH_TIMEZONE, Time.class);
        put(java.sql.Types.TIMESTAMP, Timestamp.class);
        put(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, Timestamp.class);
        put(java.sql.Types.DATE, java.sql.Date.class);
        put(java.sql.Types.BLOB, Object.class);
        put(java.sql.Types.DATALINK, Object.class);
        put(java.sql.Types.DISTINCT, Object.class);
        put(java.sql.Types.JAVA_OBJECT, Object.class);
        put(java.sql.Types.NULL, Object.class);
        put(java.sql.Types.REF, Object.class);
        put(java.sql.Types.ROWID, Object.class);
        put(java.sql.Types.STRUCT, Object.class);
        put(java.sql.Types.OTHER, Object.class);
        put(java.sql.Types.REF_CURSOR, Object.class);
        put(java.sql.Types.ARRAY, Object[].class);
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    public static SQLTypeMapping getInstance() {
        if (instance == null) {
            instance = new SQLTypeMapping();
        }

        return instance;
    }

    public Class<?> getJavaClass(int sqlType) {
        return get(sqlType);
    }
}
