package com.knoldus.apache.bean.file.processing;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.log4j.Logger;

public class MapElementMainTest {
	
	private static final Logger LOGGER = Logger.getLogger(FilterExampleMainTest.class);

	public static void main(String[] args) {
		// creating pipeline
		LOGGER.info("creating pipeline");
		Pipeline pipeline = Pipeline.create();

		// reading file using pipeline
		LOGGER.info("reading file");
		PCollection<String> applyFileReading = pipeline
				.apply(TextIO.read().from("src/main/resources/input_files/customer.csv"));

		// Transforming each element of file using map element transformation
		LOGGER.info("applying transformation");
		PCollection<String> applyMapTrnformation = applyFileReading
				.apply(
						MapElements.into(TypeDescriptors.strings())
						.via((String str) -> str.toUpperCase())
						);
		
		//writing transformed element into a file
		LOGGER.info("writing file");
		applyMapTrnformation.apply(TextIO.write()
				.to("src/main/resources/output_files/output_customer_csv")
				.withNumShards(1)
				.withSuffix(".csv")
				);
		
		//starting the pipeline
		LOGGER.info("starting pipeline");
		pipeline.run().waitUntilFinish();

	}

}
