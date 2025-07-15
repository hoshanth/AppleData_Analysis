# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast, collect_set, size, array_contains, sum, desc, row_number, avg, datediff
from pyspark.sql.types import FloatType


class Transformer:
    def __init__(self):
        pass

    def transform(self, inputDFs):
        pass

class AirpodsAfterIphoneTransformer(Transformer):

    def transform(self, inputDFs):
        """
        Customers who have bought Airpods after buying the iPhone
        """

        transactionInputDF = inputDFs.get("transactionInputDF")

        print("transactionInputDF in transform")

        transactionInputDF.show()

        windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

        transformedDF = transactionInputDF.withColumn(
            "next_product_name", lead("product_name").over(windowSpec)
        )

        print("Airpods after buying iphone")
        transformedDF.orderBy("customer_id", "transaction_date", "product_name").show()

        filteredDF = transformedDF.filter(
            (col("product_name") == "iPhone") & (col("next_product_name") == "AirPods")
        )

        filteredDF.orderBy("customer_id", "transaction_date", "product_name").show()

        customerInputDF = inputDFs.get("customerInputDF")

        customerInputDF.show()
        filteredDF.show()

        joinDF =  customerInputDF.join(
           broadcast(filteredDF),
            "customer_id"
        )

        print("JOINED DF")
        joinDF.show()

        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )


class OnlyAirpodsAndIphone(Transformer):

    def transform(self, inputDFs):
        """
        Customer who have bought only iPhone and Airpods nothing else
        """

        transactionInputDF = inputDFs.get("transactionInputDF")

        print("transactionInputDF in transform")

        groupedDF = transactionInputDF.groupBy("customer_id").agg(
            collect_set("product_name").alias("products")
        )

        print("Grouped DF")
        groupedDF.show()

        filteredDF = groupedDF.filter(
            (array_contains(col("products"), "iPhone")) &
            (array_contains(col("products"), "AirPods")) & 
            (size(col("products")) == 2)
        )
        
        print("Only Airpods and iPhone")
        filteredDF.show()

        customerInputDF = inputDFs.get("customerInputDF")

        customerInputDF.show()

        joinDF =  customerInputDF.join(
           broadcast(filteredDF),
            "customer_id"
        )

        print("JOINED DF")
        joinDF.show()

        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )

class Top3SellingProd(Transformer):
    def transform(self, inputDFs):
        """
        TOP3 selling products in each category
        """

        productInputDF = inputDFs.get("productsInputDF")

        print("productInputDF in transform")

        productInputDF = productInputDF.withColumn("price", col("price").cast(FloatType()))

        groupedDF = productInputDF.groupBy("category","product_name").agg(
            sum("price").alias("sales")
        )

        print("Grouped DF")
        groupedDF.show()

        windowSpec = Window.partitionBy("category").orderBy(desc("sales"))

        # Rank and filter top 3
        rankedDF = groupedDF.withColumn("rank", row_number().over(windowSpec)) \
                            .filter(col("rank") <= 3)
        
        print("top3 from each category")
        rankedDF.show()

        return rankedDF.select(
            "category","product_name","sales"
        )

        
class avgtimeforairpodsafteriphone(Transformer):
        def transform(self, inputDFs):
            """
            avgtimeforairpodsafteriphonepurchase
            """

            transactionInputDF = inputDFs.get("transactionInputDF")

            print("transactionInputDF in transform")

            transactionInputDF.show()

            windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

            transformedDF = transactionInputDF \
            .withColumn("next_product_name", lead("product_name").over(windowSpec)) \
            .withColumn("next_transaction_date", lead("transaction_date").over(windowSpec))

            print("Airpods after buying iphone")
            transformedDF.orderBy("customer_id", "transaction_date", "product_name","next_product_name","next_transaction_date").show()

            filteredDF = transformedDF.filter(
                (col("product_name") == "iPhone") & (col("next_product_name") == "AirPods")
            )
            filteredDF = filteredDF.withColumn( "daysforairpods", datediff(col("next_transaction_date"), col("transaction_date")))

            filteredDF.select(avg("daysforairpods")).show()

            return filteredDF.select(
                "daysforairpods"
             )
        
