package com.github.brosander.cli.tools.filter.condition;

import com.github.brosander.cli.tools.filter.FilterCondition;
import org.apache.avro.generic.GenericData;

/**
 * Created by bryan on 2/17/15.
 */
public class EqualsString implements FilterCondition {
    private final String field;
    private final String value;

    public EqualsString(String field, String value) {
        this.field = field;
        this.value = value;
    }

    @Override
    public boolean matches(GenericData.Record record) {
        return value.equals("" + record.get(field));
    }
}
