package com.github.brosander.cli.tools.filter;

import com.github.brosander.cli.tools.filter.condition.EqualsString;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bryan on 2/17/15.
 */
public class Filter {
    private final FilterCondition filterCondition;
    private static final Logger LOGGER = LoggerFactory.getLogger(Filter.class);

    public Filter(FilterCondition filterCondition) {
        this.filterCondition = filterCondition;
    }

    public void run() {
        DataFileStream<GenericData.Record> dataFileStream = null;
        DataFileWriter<GenericData.Record> dataFileWriter = null;
        try {
            dataFileStream = new DataFileStream<GenericData.Record>(System.in, new GenericDatumReader<GenericData.Record>());
            dataFileWriter = new DataFileWriter<GenericData.Record>(new SpecificDatumWriter<GenericData.Record>(dataFileStream.getSchema()));
            dataFileWriter.create(dataFileStream.getSchema(), System.out);
            for (GenericData.Record record : dataFileStream) {
                if (filterCondition.matches(record)) {
                    dataFileWriter.append(record);
                }
            }
            dataFileWriter.close();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            if ( dataFileWriter != null ) {
                try {
                    dataFileWriter.close();
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            if ( dataFileStream != null ) {
                try {
                    dataFileStream.close();
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }
    }

    public static void main(String[] args) throws ParseException, IOException {
        Options options = new Options();
        options.addOption("t", "type", true, "The type of match (eq)");
        options.addOption("f", "field", true, "The field to match");
        options.addOption("v", "value", true, "The value to match");
        CommandLineParser commandLineParser = new GnuParser();
        CommandLine commandLine = commandLineParser.parse(options, args);
        Filter filter = null;
        if ("eq".equals(commandLine.getOptionValue('t'))) {
            filter = new Filter(new EqualsString(commandLine.getOptionValue('f'), commandLine.getOptionValue('v')));
        }
        filter.run();
    }
}
