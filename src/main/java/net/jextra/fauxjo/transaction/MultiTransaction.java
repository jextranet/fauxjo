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
import java.util.*;

/**
 * Multiple {@link TransactionInterface}s tied together. All should be committed and rolled back together.
 */
public class MultiTransaction implements TransactionInterface
{
    // ============================================================
    // Fields
    // ============================================================

    private ArrayList<TransactionInterface> transactions;

    // ============================================================
    // Constructors
    // ============================================================

    public MultiTransaction( Connection... connections )
    {
        transactions = new ArrayList<>();

        for ( Connection conn : connections )
        {
            transactions.add( new Transaction( conn ) );
        }
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    @Override
    public void finish( boolean commit )
    {
        if ( commit )
        {
            commit();
        }
        else
        {
            rollback();
        }
    }

    @Override
    public void rollback()
    {
        for ( TransactionInterface trans : transactions )
        {
            trans.rollback();
        }
    }

    @Override
    public void commit()
    {
        for ( TransactionInterface trans : transactions )
        {
            trans.commit();
        }
    }

    @Override
    public void close()
    {
        for ( TransactionInterface trans : transactions )
        {
            trans.close();
        }
    }

    public TransactionInterface getTransaction( int index )
    {
        return transactions.get( index );
    }

    @Override
    public Connection getConnection()
    {
        return getConnection( 0 );
    }

    public Connection getConnection( int index )
    {
        TransactionInterface trans = getTransaction( index );

        return trans == null ? null : trans.getConnection();
    }
}
