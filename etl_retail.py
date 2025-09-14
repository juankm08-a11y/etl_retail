from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, min, max, count,desc, when,  month, desc as spark_round

spark = (
    SparkSession.builder \
    .appName("ETL Online Retail") \
    .getOrCreate()
)

df = (
    spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .option("dataAddress","'Sheet1'!A1")
    .load("data/OnlineRetail.csv")
)

print(f"Estructura del dataset:\n")
df.printSchema()

print(f"Primeros registros:\n")
df.show()

df = df.select("InvoiceNo","StockCode","Description","Quantity",
               "InvoiceDate","UnitPrice","CustomerID","Country")

total_facturas = df.select("InvoiceNo").distinct().count()
print(f"Número total de facturas: {total_facturas}")

clientes_unicos = df.select("CustomerID").distinct().count()
print(f"Número de clientes únicos: {clientes_unicos}")

df = df.withColumn("Total",col("Quantity")*col("UnitPrice"))

ingreso_total = df.agg(sum(col("Total")).alias("IngresoTotal")).collect()[0]["IngresoTotal"]
print(f"Ingreso total: {ingreso_total}")

producto_mas_vendido = (
    df.groupBy("Description")\
    .agg(sum("Quantity").alias("TotalVendido"))\
    .orderBy(desc("TotalVendido"))\
    .limit(1)
    
)
producto_mas_vendido.show()

cliente_top = (
    df.groupBy("CustomerID")\
        .agg(sum("Total").alias("TotalComprado"))\
            .orderBy(desc("TotalComprado"))\
                .limit(1)
)

cliente_top.show()


top_paises = (
    df.filter(col("Country")!="United Kingdom")\
        .groupBy("Country")\
            .agg(sum("Total").alias("TotalComprado"))\
                .orderBy(desc("TotalComprado"))\
                    .limit(5)
                    
)

top_paises.show()

ticket_promedio = (
    df.groupBy("InvoiceNo")\
        .agg(sum("Total").alias("TotalFactura"))\
            .agg(avg("TotalFactura").alias("TicketPromedio"))\
                .collect()[0]["TicketPromedio"]
)

print(f"Ticket Promedio: {round(ticket_promedio,2)}")

productos_factura = (
    df.groupBy("InvoiceNo")\
        .agg(sum("Quantity").alias("TotalProductos"))
)

stats_productos = productos_factura.agg(
    min("TotalProductos").alias("Minimo"),
    max("TotalProductos").alias("Maximo"),
    avg("TotalProductos").alias("Promedio")
)

stats_productos.show()

df = (
    df.withColumn("Mes",month(col("InvoiceDate")))
)

ventas_por_mes = (
    df.groupBy("Mes")\
        .agg(sum("Total").alias("VentasTotales"))\
            .orderBy(desc("VentasTotales"))\
                .limit(1)
)

ventas_por_mes.show()

facturas_totales = (
    df.select("InvoiceNo").distinct().count()
)

facturas_devoluciones = (
    df.filter(col("Quantity")<0)\
        .select("InvoiceNo").distinct().count()
)

porcentaje_devoluciones = (facturas_devoluciones / facturas_totales) *100

print(f"Porcentaje de facturas con devoluciones: {round(porcentaje_devoluciones,2)}%")

producto_mas_vendido.coalesce(1).write.csv("output/producto_mas_vendido",header=True)
cliente_top.coalesce(1).write.csv("output/cliente_top",header=True)
top_paises.coalesce(1).write.csv("output/top_paises",header=True)
stats_productos.coalesce(1).write.csv("output/stats_productos",header=True)
ventas_por_mes.coalesce(1).write.csv("output/ventas_por_mes",header=True)