# Databricks notebook source
# MAGIC %run "./Reader_Format"

# COMMAND ----------

class Extractor:
    """
    Abstract class 
    """

    def __init__(self):
        pass
    def extract(self):
        pass

class AirpodsAfterIphoneExtractor(Extractor):

    def extract(self):
        """
        Implement the steps for extracting or reading the data
        """
        transactionInputDF = get_data_source(
            data_type = "csv",
            file_path="dbfs:/FileStore/appledata/Transaction_Updated.csv"
        ).get_data_frame()


        customerInputDF = get_data_source(
            data_type = "delta",
            file_path="default.customer_delta_table_persist"
        ).get_data_frame()
        
        productsInputDF = get_data_source(
            data_type = "csv",
            file_path="dbfs:/FileStore/appledata/Products_Updated.csv"
        ).get_data_frame()
        
        
        inputDFs = {
            "transactionInputDF": transactionInputDF,
            "customerInputDF": customerInputDF,
            "productsInputDF": productsInputDF
        }

        return inputDFs

# COMMAND ----------

