import re
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# 符号表
SYMBOLS = {'》': '《', '”': '“', '"': '"', ')': '(', '）': '（'}
SYMBOLS_L, SYMBOLS_R = list(SYMBOLS.values()), list(SYMBOLS.keys())


def isBracketPair(s):
    flag = False
    arr = []
    for c in s:
        if c in SYMBOLS_L:
            if c in arr:
                return False
            elif flag:
                return False
            else:
                # 左符号入栈
                flag = True
                arr.append(c)
        elif c in SYMBOLS_R:
            # 右符号要么出栈，要么匹配失败
            if arr and arr[-1] == SYMBOLS[c]:
                arr.pop()
            else:
                return False

    return not arr


def is_grabled_code(text):
    regex_str = "^(([\u4e00-\u9fa5_a-zA-Z0-9\\)\\(）（》《”“\"\\s])*([-，,-：:·]?)([\u4e00-\u9fa5_a-zA-Z0-9\\)\\(）（》《”“\"\\s]*))$"
    match_obj = re.match(regex_str, text)
    if match_obj is None:
        return False
    else:
        return True


def analysis_row(row):
    title = row.doc_title + row.attribute
    flag = 0
    if is_grabled_code(row.doc_title) == False or isBracketPair(row.doc_title) == False:
        flag = 1
    if is_grabled_code(row.doc_title) == False or isBracketPair(row.doc_title) == False:
        flag = 1
    return row.id, row.doc_title, row.attribute, row.status, row.merge_to, flag,


def run():
    spark = SparkSession \
        .builder \
        .enableHiveSupport() \
        .appName("WikiDocGrabledCode") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()

    conf = SparkConf()
    date = str(conf.get("spark.biz.date"))
    task_type = "grabled_code"

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

