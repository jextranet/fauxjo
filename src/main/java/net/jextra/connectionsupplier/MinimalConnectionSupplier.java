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

package net.jextra.connectionsupplier;

import java.sql.*;
import java.util.*;
import javax.sql.*;

public class MinimalConnectionSupplier implements ConnectionSupplier {
    // ============================================================
    // Fields
    // ============================================================

    private Connection connection;
    private DataSource dataSource;
    private HashMap<String,PreparedStatement> preparedStatements;
    private HashMap<String,CallableStatement> preparedCalls;

    // ============================================================
    // Constructors
    // ============================================================

    public MinimalConnectionSupplier() {
        preparedStatements = new HashMap<>();
        preparedCalls = new HashMap<>();
    }

    public MinimalConnectionSupplier(Connection conn) {
        this();

        setConnection(conn);
    }

    public MinimalConnectionSupplier(DataSource ds) throws SQLException {
        this();

        setDataSource(ds);
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    public void setDataSource(DataSource ds) throws SQLException {
        dataSource = ds;
        connection = dataSource.getConnection();
    }

    public void setConnection(Connection conn) {
        connection = conn;
        dataSource = null;
    }

    @Override
    public Connection getConnection() throws SQLException {
        validateConnection();

        return connection;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        validateConnection();

        PreparedStatement statement = preparedStatements.get(sql);
        if (statement == null || statement.isClosed()) {
            if (SQLInspector.isInsertStatement(sql)) {
                statement = getConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            } else {
                statement = getConnection().prepareStatement(sql);
            }
            preparedStatements.put(sql, statement);
        }

        return statement;
    }

    @Override
    public PreparedStatement prepareCall(String sql) throws SQLException {
        validateConnection();

        CallableStatement call = preparedCalls.get(sql);
        if (call == null || call.isClosed()) {
            call = getConnection().prepareCall(sql);
            preparedCalls.put(sql, call);
        }

        return call;
    }

    @Override
    public boolean close() throws SQLException {
        for (PreparedStatement statement : preparedStatements.values()) {
            if (!statement.isClosed()) {
                statement.close();
            }
        }
        preparedStatements.clear();

        for (CallableStatement call : preparedCalls.values()) {
            if (!call.isClosed()) {
                call.close();
            }
        }
        preparedCalls.clear();

        if (connection == null) {
            return false;
        }

        if (connection != null) {
            connection.close();
        }

        connection = null;

        return true;
    }

    public void validateConnection() throws SQLException {
        if (connection != null && !connection.isValid(1000)) {
            close();

            // If there is a dataSource, go get a new connection.
            if (dataSource != null) {
                connection = dataSource.getConnection();
            }
        }
    }
}
