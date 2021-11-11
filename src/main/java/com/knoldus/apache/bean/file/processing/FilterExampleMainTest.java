package com.knoldus.apache.bean.file.processing;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.log4j.Logger;

class MyFilter implements SerializableFunction<String, Boolean>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Boolean apply(String input) {
		return input.contains("Los Angeles");
	}
	
	
}

public class FilterExampleMainTest {
	
	private static final Logger LOGGER = Logger.getLogger(FilterExampleMainTest.class);

	public static void main(String[] args) {
		try {
			//create pipeline
			LOGGER.info("creating pipeline");
			Pipeline pipeline = Pipeline.create();
			
			//read the file using pipeline
			LOGGER.info("reading file");
			PCollection<String> pCollectionCustomerFile = pipeline.apply(TextIO
					.read()
					.from("src/main/resources/input_files/customer_pardo.csv")
					);
			
			//applying the custom filter to filter out the specific records from the customer file
			LOGGER.info(" applying filter to the file data");
			PCollection<String> pCollectionFilter = pCollectionCustomerFile.apply(Filter.by(new MyFilter()));
			
			//writing the file record after filter into a file
			LOGGER.info("writing file");
			pCollectionFilter.apply(TextIO
					.write()
					.to("src/main/resources/output_files/filter_customer_pardo.csv")
					.withHeader("id,name,lastName,city")
					.withNumShards(1)
					.withSuffix(".csv")
					);
			
			//run the pipeline
			LOGGER.info("starting pipeline");
			pipeline.run().waitUntilFinish();
		} catch (Throwable e) {
			System.out.println("error is " + e);
		}

	}

}
