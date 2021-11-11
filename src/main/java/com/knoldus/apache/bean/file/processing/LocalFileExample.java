package com.knoldus.apache.bean.file.processing;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

import com.knoldus.apache.bean.file.processing.service.MyOption;

public class LocalFileExample {

	public static void main(String[] args) {
		
		
		//creating my own options
		MyOption myOptions = PipelineOptionsFactory.fromArgs(args)
				.withValidation()
				.as(MyOption.class);
		
		// Create and set our Pipeline Options.
		  //PipelineOptions options = PipelineOptionsFactory.create();

		//create pipeline object
		Pipeline pipeline = Pipeline.create(myOptions);
		
		//read the csv file
		
		  PCollection<String> inputApply = pipeline.apply(TextIO .read()
		  .from(myOptions.getInputFile()));
		 
		
		

		//write content into CSV file
		
		  inputApply.apply(TextIO .write() .to(myOptions.getOutputFile())
		  .withNumShards(1) .withSuffix(myOptions.getExtn()));
		 
		
		//running the pipeline
		pipeline.run();
		
	}

}
