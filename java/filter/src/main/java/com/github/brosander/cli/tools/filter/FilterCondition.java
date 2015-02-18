package com.github.brosander.cli.tools.filter;

import org.apache.avro.generic.GenericData;

/**
 * Created by bryan on 2/17/15.
 */
public interface FilterCondition {
    public boolean matches(GenericData.Record record);
}
