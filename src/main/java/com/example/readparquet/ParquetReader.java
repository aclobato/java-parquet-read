package com.example.readparquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.math.genetics.GeneticAlgorithm;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ParquetReader {
	
	public void readSquema(String path) throws IOException {
		Configuration conf = new Configuration();
		ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, new Path(path), ParquetMetadataConverter.NO_FILTER);
		log.info(readFooter.getFileMetaData().getSchema().toString());
	}
	
	public void readColumn(String path, String columnName) throws IOException {
		ParquetMetadata readFooter = ParquetFileReader.readFooter(new Configuration(), new Path(path), ParquetMetadataConverter.NO_FILTER);
		MessageType originalSchema = readFooter.getFileMetaData().getSchema();
		
		MessageType schema = new MessageType(originalSchema.getName(), originalSchema.getFields().get(originalSchema.getFieldIndex(columnName)));
		Configuration conf = new Configuration();
		conf.set(ReadSupport.PARQUET_READ_SCHEMA, schema.toString());
		
		org.apache.parquet.hadoop.ParquetReader<Group> reader = org.apache.parquet.hadoop.ParquetReader
			.builder(new GroupReadSupport(), new Path(path))
			.withConf(conf)
			.build();
		Group group;
		while ((group = reader.read()) != null) {
			if (!group.toString().isEmpty()) {
				log.info(group.getString(columnName, 0));
			}
		}
	}

}
