package com.knoldus.apache.bean.file.processing;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.log4j.Logger;

public class LocalFileExampleWithStaticFilePath {
	
	private static final Logger LOGGER = Logger.getLogger(LocalFileExampleWithStaticFilePath.class);

	public static void main(String[] args) {

		// Create and set our Pipeline Options.
		LOGGER.info("creating pipeline");
		PipelineOptions options = PipelineOptionsFactory.create();

		// create pipeline object
		Pipeline pipeline = Pipeline.create(options);

		// read the csv file
		LOGGER.info("reading file");
		PCollection<String> inputApply = pipeline
				.apply(TextIO.read().from("src/main/resources/input_files/input.csv"));

		// write content into CSV file
		LOGGER.info("writing file");
		inputApply.apply(TextIO.write().to("src/main/resources/output_files/output.csv")
				.withNumShards(1).withSuffix(".csv"));

		// running the pipeline
		LOGGER.info("starting pipeline");
		pipeline.run();
	}

}
