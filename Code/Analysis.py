# Databricks notebook source
# MAGIC %run "./transform"
# MAGIC

# COMMAND ----------

# MAGIC %run "./Extractor"

# COMMAND ----------

# MAGIC %run "./Loader"

# COMMAND ----------

class AirpodsAfterIphone:
    """
    ETL pipeline to generate the data for all customers who have bought Airpods just after buying iPhone
    """ 
    def __init__(self):
        pass

    def runner(self):

        # Step 1: Extract all required data from different source
        inputDFs = AirpodsAfterIphoneExtractor().extract()

        # Step 2: Implement the Transformation logic
        # Customers who have bought Airpods after buying the iPhone
        firstTransformedDF = AirpodsAfterIphoneTransformer().transform(inputDFs)

        # Step 3: Load all required data to differnt sink
        AirPodsAfterIphoneLoader(firstTransformedDF).sink()
        


# COMMAND ----------

class only_iphone:
    """
    ETL pipeline to generate the data for all customers who have bought only iPhone and Customers
    """ 
    def __init__(self):
        pass

    def runner(self):

        # Step 1: Extract all required data from different source
        inputDFs = AirpodsAfterIphoneExtractor().extract()

        # Step 2: Implement the Transformation logic
        # Customers who have bought Airpods after buying the iPhone
        onlyAirpodsAndIphoneDF = OnlyAirpodsAndIphone().transform(inputDFs)

        # Step 3: Load all required data to differnt sink
        OnlyAirpodsAndIPhoneLoader(onlyAirpodsAndIphoneDF).sink()
        

# COMMAND ----------

class top3selling_prod_in_category:
    """
    ETL pipeline to generate the data for TOP3 selling products in each category
    """ 
    def __init__(self):
        pass

    def runner(self):

        # Step 1: Extract all required data from different source
        inputDFs = AirpodsAfterIphoneExtractor().extract()

        # Step 2: Implement the Transformation logic
        # TOP3 selling products in each category
        Top3 = Top3SellingProd().transform(inputDFs)

        # Step 3: Load all required data to differnt sink
        Top3SellingProdLoader(Top3).sink()




# COMMAND ----------

class avgtimeforairpods:
    """
    ETL pipeline to generate the data for avg time delay buying airpods after buying iphone
    """ 
    def __init__(self):
        pass

    def runner(self):

        # Step 1: Extract all required data from different source
        inputDFs = AirpodsAfterIphoneExtractor().extract()

        # Step 2: Implement the Transformation logic
        # TOP3 selling products in each category
        avgtime = avgtimeforairpodsafteriphone().transform(inputDFs)

        # Step 3: Load all required data to differnt sink
        avgtimeforairpodsLoader(avgtime).sink()


# COMMAND ----------

class WorkFlowRunner:

    def __init__(self, name):
        self.name = name

    def runner(self):
        if self.name == "firstWorkFlow":
            return AirpodsAfterIphone().runner()
        elif self.name == "secondWorkFlow":
            return only_iphone().runner()
        elif self.name == "thirdWorkFlow":
            return top3selling_prod_in_category().runner()
        elif self.name == "fourthWorkFlow":
            return avgtimeforairpods().runner()
        else:
            raise ValueError(f"Not Implemented for {self.name}")

workflow_names = [
    "firstWorkFlow",
    "secondWorkFlow",
    "thirdWorkFlow",
    "fourthWorkFlow"
]

for wf in workflow_names:
    print(f"\nRunning {wf}...")
    WorkFlowRunner(wf).runner()
