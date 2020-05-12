import re
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


def is_quote(text):
    regex_str = "^(\"|”|“)+(.*)(\"|”|“)+$"
    match_obj = re.match(regex_str, text)
    if match_obj is None:
        return False
    else:
        return True


def analysis_row(row):
    flag = 0
    if is_quote(row.doc_title) == True or is_quote(row.doc_title) == True:
        flag = 1
    return row.id, row.doc_title, row.attribute, row.status, row.merge_to, flag,


def run():
    spark = SparkSession \
        .builder \
        .enableHiveSupport() \
        .appName("WikiDocQuoteCode") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()

    conf = SparkConf()
    date = str(conf.get("spark.biz.date"))
    task_type = "quote"

    df = spark.sql(
        f"SELECT id,doc_title, attribute, status,merge_to  FROM toutiao_baike.wiki_doc "
        f"WHERE  date ={date}  "
    )

    input_data = df.rdd.map(lambda row: analysis_row(row))

    df = spark \
        .createDataFrame(input_data,
                         ["doc_id", "doc_title", "attribute", "status", "merge_to", "flag"])

    # ["doc_id", "doc_title", "attribute", "search_pv", "extra_num", "extra_1", "extra_2"])

    df.select("doc_id", "doc_title", "attribute", "status", "merge_to", "flag",
              "flag") \
        .withColumn("date", lit(date)) \
        .withColumn("task_type", lit(task_type)) \
        .write.insertInto("toutiao_baike.mds_midas_doc_stat_tmp_day", True)
    spark.stop()


if __name__ == '__main__':
    run()
