/*
 * Copyright (C) jextra.net.
 *
 * This file is part of the jextra.net software.
 *
 * The jextra software is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * The jextra software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with the jextra software; if not, write to the Free
 * Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307 USA.
 */

package net.jextra.fauxjo.transaction;

import java.sql.*;

/**
 * A clean convience class to manage a typical single database "transaction".
 */
public class Transaction implements TransactionInterface
{
    // ============================================================
    // Fields
    // ============================================================

    private TransactionListener listener;
    private Connection connection;
    private boolean savedAutoCommit;
    private Savepoint savepoint;

    // ============================================================
    // Constructors
    // ============================================================

    public Transaction( Connection connection )
    {
        this( connection, (TransactionListener) null );
    }

    public Transaction( Connection connection, TransactionListener listener )
    {
        this.connection = connection;
        this.listener = listener;

        //
        // Get current autocommit setting then set autocommit off.
        //
        try
        {
            savedAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit( false );
        }
        catch ( SQLException ex )
        {
            // Wrap in RuntimeException because very unlikely to occur anyways and annoying to
            // have to have constructor for creating transaction have to be caught.
            throw new RuntimeException( ex );
        }
    }

    public Transaction( Connection connection, String savepointName )
    {
        this.connection = connection;

        //
        // Get current autocommit setting then set autocommit off.
        // Create savepoint.
        //
        try
        {
            savedAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit( false );

            savepoint = connection.setSavepoint( savepointName );
        }
        catch ( SQLException ex )
        {
            // Wrap in RuntimeException because very unlikely to occur anyways and annoying to
            // have constructor for creating transaction have to be caught.
            throw new RuntimeException( ex );
        }
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    public Savepoint getSavepoint()
    {
        return savepoint;
    }

    @Override
    public void rollback()
    {
        try
        {
            if ( listener != null )
            {
                listener.trace( this, "Rolling back transaction" );
            }

            if ( savepoint != null )
            {
                connection.rollback( savepoint );
            }
            else
            {
                connection.rollback();
            }
            connection.setAutoCommit( savedAutoCommit );
        }
        catch ( SQLException ex )
        {
            throw new RuntimeException( ex );
        }
    }

    @Override
    public void commit()
    {
        try
        {
            if ( listener != null )
            {
                listener.trace( this, "Committing transaction" );
            }

            connection.commit();
            connection.setAutoCommit( savedAutoCommit );
        }
        catch ( SQLException ex )
        {
            throw new RuntimeException( ex );
        }
    }

    /**
     * This should usually be called in a finally block, or anywhere it is unknown if a {@link #commit} or
     * {@link #rollback} has been called. This will also restore the connection's autocommit state back to its
     * previous value before the transaction.
     */
    @Override
    public void close()
    {
        try
        {
            if ( listener != null )
            {
                listener.trace( this, "Closing out transaction" );
            }

            if ( connection != null && !connection.getAutoCommit() && savedAutoCommit )
            {
                // Note, this rollback may not do anything if a commit just occured before it.
                connection.rollback();
                connection.setAutoCommit( savedAutoCommit );
            }
        }
        catch ( SQLException ex )
        {
            throw new RuntimeException( ex );
        }
    }

    @Override
    public Connection getConnection()
    {
        return connection;
    }

    // ============================================================
    // Inner Classes
    // ============================================================

    public interface TransactionListener
    {
        void trace( Transaction trans, String message );
    }
}
