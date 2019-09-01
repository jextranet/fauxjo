/*
 * Copyright (C) Elsevier
 * All Rights Reserved.
 */

package net.jextra.connectionsupplier;

import java.sql.*;

/**
 * A clean convience class to manage a database "transaction".
 */
public class Transaction {
    // ============================================================
    // Fields
    // ============================================================

    private Connection connection;
    private boolean savedAutoCommit;
    private Savepoint savepoint;

    // ============================================================
    // Constructors
    // ============================================================

    public Transaction(Connection connection) {
        this.connection = connection;

        //
        // Save autocommit value and set to false.
        //
        try {
            savedAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
        } catch (SQLException ex) {
            // Wrap in RuntimeException because very unlikely to occur anyways and annoying to
            // have constructor for creating transaction have to be caught.
            throw new RuntimeException(ex);
        }
    }

    public Transaction(Connection connection, String savepointName) {
        this.connection = connection;

        //
        // Save autocommit value and set to false.
        // Create savepoint.
        //
        try {
            savedAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            savepoint = connection.setSavepoint(savepointName);
        } catch (SQLException ex) {
            // Wrap in RuntimeException because very unlikely to occur anyways and annoying to
            // have constructor for creating transaction have to be caught.
            throw new RuntimeException(ex);
        }
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    public Savepoint getSavepoint() {
        return savepoint;
    }

    public void rollback() {
        try {
            if (savepoint != null) {
                connection.rollback(savepoint);
            } else {
                connection.rollback();
            }
            connection.setAutoCommit(savedAutoCommit);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void commit() {
        try {
            connection.commit();
            connection.setAutoCommit(savedAutoCommit);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * This should usually be called in a finally block, or anywhere it is unknown if a {@link #commit} or
     * {@link #rollback} has been called. This will restore the connection's autocommit state back to its
     * previous value.
     */
    public void cleanup() {
        try {
            if (connection != null && !connection.getAutoCommit() && savedAutoCommit) {
                //                XLog.error("Transaction was not committed or rolled back.");
                //                XLog.errorf("Setting auto-commit to %b and rolling back", savedAutoCommit);
                connection.rollback();
                connection.setAutoCommit(savedAutoCommit);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection getConnection() {
        return connection;
    }
}
