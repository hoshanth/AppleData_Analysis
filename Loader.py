# Databricks notebook source
# MAGIC %run "./loader_factory"

# COMMAND ----------

class AbstractLoader:
    def __init__(self, transformedDF):
        self.transformedDF = transformedDF

    def sink(self):

        pass

class AirPodsAfterIphoneLoader(AbstractLoader):

    def sink(self):
        get_sink_source(
            sink_type = "dbfs",
            df = self.transformedDF, 
            path = "dbfs:/FileStore/tables/apple_analysis/output/airpodsAfterIphone", 
            method = "overwrite"
        ).load_data_frame()

class OnlyAirpodsAndIPhoneLoader(AbstractLoader):

    def sink(self):
        params = {
            "partitionByColumns": ["location"]
        }
        get_sink_source(
            sink_type = "dbfs_with_partition",
            df = self.transformedDF, 
            path = "dbfs:/FileStore/tables/apple_analysis/output/airpodsOnlyIphone", 
            method = "overwrite",
            params = params
        ).load_data_frame()

        get_sink_source(
            sink_type = "delta",
            df = self.transformedDF, 
            path = "default.onlyAirPodsAndIphone", 
            method = "overwrite",
        ).load_data_frame()          

class Top3SellingProdLoader(AbstractLoader):

    def sink(self):
        get_sink_source(
            sink_type = "dbfs",
            df = self.transformedDF, 
            path = "dbfs:/FileStore/tables/apple_analysis/output/Top3Prod", 
            method = "overwrite"
        ).load_data_frame() 

class avgtimeforairpodsLoader(AbstractLoader):

    def sink(self):
        get_sink_source(
            sink_type = "dbfs",
            df = self.transformedDF, 
            path = "dbfs:/FileStore/tables/apple_analysis/output/avgtimeforairpods", 
            method = "overwrite"
        ).load_data_frame() 