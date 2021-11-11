package com.knoldus.apache.bean.file.processing;

import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.log4j.Logger;

public class SideInputExampleMainTest {
	
	private static final Logger LOGGER = Logger.getLogger(SideInputExampleMainTest.class);

	public static void main(String[] args) {
		//create pipeline
		LOGGER.info("creating pipeline");
		Pipeline pipeline = Pipeline.create();
		
		//read file using pipeline
		LOGGER.info("reading file");
		PCollection<String> returnFileApply = pipeline.apply(TextIO
				.read()
				.from("src/main/resources/input_files/return.csv")
				);
		
		//processing the file loaded and getting `0th` and `1st` element from each row
		LOGGER.info("processing the file loaded and getting `0th` and `1st` element from each row");
		PCollection<KV<String, String>> pCollectionKV = returnFileApply.apply(
				
				ParDo.of(new DoFn<String, KV<String,String>>() {
					
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void process(ProcessContext context) {
						String[] split = context.element().split(",");
						context.output(KV.of(split[0], split[1]));
					}
				
				    })
				
				);
		
		PCollectionView<Map<String, String>> pCollectionMap = pCollectionKV.apply(View.asMap());
		LOGGER.info("reading file");
		String name = pCollectionMap.getName();
		LOGGER.info("value of name is " + name);
		
		LOGGER.info("starting pipeline");
		pipeline.run().waitUntilFinish();
		
		

	}

}
