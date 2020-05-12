import heapq
import random

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


def a_res(samples, m):
    """
    :samples: [(item, weight), ...]
    :k: number of selected items
    :returns: [(item, weight), ...]
    """

    heap = []  # [(new_weight, item), ...]
    for sample in samples:
        wi = sample[1]
        ui = random.uniform(0, 1)
        ki = ui ** (1 / wi)

        if len(heap) < m:
            heapq.heappush(heap, (ki, sample))
        elif ki > heap[0][0]:
            heapq.heappush(heap, (ki, sample))

            if len(heap) > m:
                heapq.heappop(heap)

    return [item[1] for item in heap]


def run():
    spark = SparkSession \
        .builder \
        .enableHiveSupport() \
        .appName("WikiDocCRandomPv") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()

    conf = SparkConf()
    date = str(conf.get("spark.biz.date"))
    task_type = "cpv"

    df = spark.sql(
        f"SELECT doc_id, click_pv  FROM toutiao_baike.mds_midas_doc_c_pv_week "
        f"WHERE  date =20200511  and click_pv>0 order by click_pv desc limit 10000000"
    )

    pv_weight_collect = a_res(df.collect(), 400)
    df_new = spark \
        .createDataFrame(pv_weight_collect, ["rel_id", "pv"])

    df_catagory = spark.sql(
        f"SELECT doc_id, doc_title,attribute,level_first  FROM toutiao_baike.mds_midas_doc_category_statistic_day "
        f"WHERE  date =20200506"
    )
    df_new = df_new.join(df_catagory, df_new["rel_id"] == df_catagory["doc_id"], 'left_outer')
    df_new.select("doc_id", "doc_title", "attribute", "pv", "pv", "level_first",
                  "attribute") \
        .withColumn("date", lit(date)) \
        .withColumn("task_type", lit(task_type)) \
        .write.insertInto("toutiao_baike.mds_midas_doc_stat_tmp_day", True)
    spark.stop()


if __name__ == '__main__':
    run()
