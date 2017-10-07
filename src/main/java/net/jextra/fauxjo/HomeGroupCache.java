/*
 * Copyright (C) Elsevier
 * All Rights Reserved.
 */

package net.jextra.fauxjo;

import java.util.*;

/**
 * Delegate class to be used in a {@link HomeGroup} class to
 * manage its instances.
 */
public class HomeGroupCache<T extends HomeGroup> {
    // ============================================================
    // Fields
    // ============================================================

    public static String DEFAULT_INSTANCE = "_default_";

    private Map<String,T> instances;
    private Class<T> clazz;

    // ============================================================
    // Constructors
    // ============================================================

    public HomeGroupCache(Class<T> clazz) {
        this.clazz = clazz;
        instances = new HashMap<>();
    }

    // ============================================================
    // Methods
    // ============================================================

    // ----------
    // public
    // ----------

    public T use(String instanceName) throws FauxjoException {
        if (instanceName == null) {
            instanceName = DEFAULT_INSTANCE;
        }

        T homeGroup = instances.get(instanceName);
        if (homeGroup == null) {
            try {
                homeGroup = clazz.newInstance();
                instances.put(instanceName, homeGroup);
            } catch (Exception e) {
                throw new FauxjoException(e);
            }

            if (homeGroup == null) {
                throw new FauxjoException(String.format("Unable to create HomeGroup for class %s", clazz.getName()));
            }
        }
        return homeGroup;
    }

    public T use() throws FauxjoException {
        return use(DEFAULT_INSTANCE);
    }

    public void setInstance(String instanceName, T schema) {
        instances.put(instanceName, schema);
    }

    public void setInstance(T schema) {
        setInstance(DEFAULT_INSTANCE, schema);
    }
}
