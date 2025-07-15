# AppleData_Analysis

This repository contains Databricks notebooks (exported as HTML) and accompanying Python code that outlines a comprehensive ETL (Extract, Transform, Load) pipeline for "Appledata_Analysis". The project is designed to process and analyze customer purchase data, specifically focusing on Apple products, through a series of modular and specialized workflows.

Project Structure
The project is organized into several key components, each represented by a Databricks notebook or a Python class:

Extractor.html: Handles the extraction of raw data. This notebook is responsible for connecting to source systems and pulling the necessary data for analysis.

transform.html: Performs data transformation. This stage involves cleaning, reformatting, and enriching the extracted data to prepare it for loading and analysis.

Loader.html: Manages the loading of transformed data into its final destination (e.g., a data warehouse or data lake).

loader_factory.html: Likely contains factory methods or utilities for creating and configuring different data loaders, promoting reusability and flexibility in the loading process.

Reader_Format.html: Defines or applies specific data reading formats. This notebook might contain logic for parsing various input data formats to ensure compatibility with the pipeline.

Analysis.html: Focuses on the analytical aspects of the data. This notebook would contain queries, visualizations, and statistical models to derive insights from the processed Apple data.

Technologies
Databricks: The primary platform for running these notebooks, leveraging Apache Spark for large-scale data processing.

Python: Used for implementing the core ETL logic and workflow orchestration.

HTML: The notebooks are exported as HTML files, making them easily viewable in a web browser.

Project Workflows
This project implements several specialized ETL pipelines, each designed to generate specific analytical datasets from customer purchase data. These pipelines are organized into distinct Python classes, orchestrated by a WorkFlowRunner.

ETL Pipeline Architecture
Each ETL pipeline follows a standard three-step process within its runner method:

Extraction: Gathers all necessary raw data from different sources using an Extractor class.

Transformation: Applies business logic and data manipulation to refine the extracted data using a Transformer class.

Loading (Sinking): Stores the processed data into a designated destination using a Loader class.

Implemented Workflows
The project includes the following specialized ETL workflows:

AirpodsAfterIphone
Purpose: This pipeline generates the data for all customers who have bought AirPods just after buying an iPhone. It identifies a specific purchase sequence.

Process:

Extraction: Utilizes AirpodsAfterIphoneExtractor().extract() to retrieve raw customer purchase data, likely including timestamps and product IDs.

Transformation: Employs AirpodsAfterIphoneTransformer().transform() to process the extracted data. This transformation logic would identify customers who bought an iPhone and subsequently purchased AirPods, potentially within a specific timeframe or as their next immediate purchase.

Loading: Uses AirPodsAfterIphoneLoader().sink() to store the resulting dataset, which would list the customers fitting this purchase pattern.

only_iphone
Purpose: This pipeline generates data for customers who have bought only an iPhone, or potentially, a specific combination of AirPods and iPhone purchases as suggested by the OnlyAirpodsAndIphone transformer.

Process:

Extraction: Reuses AirpodsAfterIphoneExtractor().extract() to get the initial purchase data.

Transformation: Applies OnlyAirpodsAndIphone().transform() to filter the data. This likely identifies customers whose purchase history is limited to iPhones, or a specific pattern of only iPhones and AirPods, excluding other Apple products.

Loading: Employs OnlyAirpodsAndIPhoneLoader().sink() to store the data for these specific customer segments.

top3selling_prod_in_category
Purpose: This pipeline generates the data for the top 3 selling products in each product category. This is a common business intelligence task to identify high-performing items.

Process:

Extraction: Reuses AirpodsAfterIphoneExtractor().extract() to gather raw sales and product category data.

Transformation: Uses Top3SellingProd().transform() to aggregate sales data by product and category, then ranks products within each category to identify the top 3. This would involve grouping, summing sales, and applying ranking functions.

Loading: Utilizes Top3SellingProdLoader().sink() to save the resulting dataset, containing the top-selling products per category.

avgtimeforairpods
Purpose: This pipeline generates data for the average time delay between a customer buying an iPhone and then subsequently buying AirPods. This provides insights into customer upgrade or complementary product purchase behavior.

Process:

Extraction: Reuses AirpodsAfterIphoneExtractor().extract() to obtain the necessary timestamped purchase data for both iPhones and AirPods.

Transformation: Employs avgtimeforairpodsafteriphone().transform() to compute the time difference between an iPhone purchase and a subsequent AirPods purchase for each relevant customer, and then calculates the average of these time differences.

Loading: Uses avgtimeforairpodsLoader().sink() to store this calculated average time delay.

Workflow Orchestration (WorkFlowRunner)
The WorkFlowRunner class acts as a centralized orchestrator, allowing specific ETL pipelines to be executed based on a provided name:

__init__(self, name): Initializes the runner with the name of the workflow to execute.

runner(self): This method serves as the main entry point for executing a workflow. It checks the name parameter and dynamically calls the runner() method of the corresponding pipeline class.

"firstWorkFlow": Executes the AirpodsAfterIphone pipeline.

"secondWorkFlow": Executes the only_iphone pipeline.

"thirdWorkFlow": Executes the top3selling_prod_in_category pipeline.

"fourthWorkFlow": Executes the avgtimeforairpods pipeline.

If an unrecognized workflow name is provided, it raises a ValueError, indicating that the requested workflow is not implemented.

Execution Example
The provided Python code snippet demonstrates how to run all defined workflows sequentially using the WorkFlowRunner:

Python

workflow_names = [
    "firstWorkFlow",
    "secondWorkFlow",
    "thirdWorkFlow",
    "fourthWorkFlow"
]

for wf in workflow_names:
    print(f"\nRunning {wf}...")
    WorkFlowRunner(wf).runner()
This loop iterates through each defined workflow name and triggers its execution, logging the progress to the console.

Getting Started
To utilize this project:

Import to Databricks: Upload the HTML files (representing the Databricks notebooks) into your Databricks workspace. Databricks can typically import these HTML files back into runnable notebooks.

Upload Python Code: Ensure the Python classes for the extractors, transformers, and loaders (e.g., AirpodsAfterIphoneExtractor, AirpodsAfterIphoneTransformer, AirpodsAfterIphoneLoader, OnlyAirpodsAndIphone, Top3SellingProd, avgtimeforairpodsafteriphone, and their respective loaders) are accessible within your Databricks environment. This could be by placing them in a Databricks library or directly in a notebook that imports them.

Configure: Review each notebook and the Python code to configure data sources, transformation logic, and loading destinations according to your specific environment and requirements.

Execute: Run the WorkFlowRunner script (or integrate its execution into a Databricks job) to trigger the desired ETL pipelines.

Usage
Run the WorkFlowRunner to execute the predefined ETL pipelines.

Examine the output of the Loader components to access the generated analytical datasets.

Open Analysis.html in Databricks to further explore and visualize the results.
