package com.knoldus.apache.bean.file.processing;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.log4j.Logger;

class CustomerFilter extends DoFn<String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(ProcessContext processContext) {
		String line = processContext.element();

		String[] split = line.split(",");

		if (split[3].equalsIgnoreCase("Los Angeles")) {
			processContext.output(line);
		}
	}

}

public class ParDoExampleMainTest {

	private static final Logger LOGGER = Logger.getLogger(ParDoExampleMainTest.class);

	public static void main(String[] args) {
		// creating pipeline
		LOGGER.info("creating pipeline");
		Pipeline pipeline = Pipeline.create();

		// reading file using pipeline
		LOGGER.info("reading file");
		PCollection<String> fileReadApply = pipeline
				.apply(TextIO.read().from("src/main/resources/input_files/customer_pardo.csv"));

		// applying the filter function on PCollection object
		LOGGER.info("applying the filter function on PCollection object");
		PCollection<String> fileApplyFilter = fileReadApply.apply(ParDo.of(new CustomerFilter()));

		// writing the PCollection element to a file
		LOGGER.info("writing file");
		fileApplyFilter.apply(TextIO.write().to("src/main/resources/input_files/output_customer_pardo.csv")
				.withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));

		LOGGER.info("starting pipeline");
		// run the pipeline
		pipeline.run().waitUntilFinish();

	}

}
