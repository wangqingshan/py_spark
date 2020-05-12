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


def analysis_row(row):
    random_weight = row.search_pv + random.randint(0, total_num)
    return row.doc_id, row.doc_title, row.doc_attribute, row.search_pv, random_weight, '', ''


def run():
    spark = SparkSession \
        .builder \
        .enableHiveSupport() \
        .appName("WikiDocZwRandomPv") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .getOrCreate()

    conf = SparkConf()
    date = str(conf.get("spark.biz.date"))
    task_type = "zwpv"
    df = spark.sql(f"""SELECT
    tt.id,
    doc_title,
    attribute,
    click_pv
FROM
    (
        SELECT
            id,
            sum(click_pv) as click_pv
        FROM
            toutiao_baike.wiki_doc
            LEFT JOIN (
                SELECT
                    regexp_replace(doc_title, '(\\[.*\\])', '') as original_title,
                    regexp_extract(doc_title, '\\[(.+)\\]', 1) as original_attribute,
                    sum(click_pv_1) as click_pv
                from
                    (
                        SELECT
                            regexp_extract(
                                regexp_replace(doc_title, '\\s*', ''),
                                '(\\[baike_hudong_structure_new\\])?(.+?)([_-](手机互动)?百科)?$',
                                2
                            ) as doc_title,
                            sum(client_click) as click_pv_1
                        from
                            search_user_data.click_through_v3_topic_instance
                        where
                            date >= '20200427'
                            and date <= '20200510'
                            and is_spam_user = 0
                            and app_name in ('website_search')
                            and (
                                doc_ala_src = 'baike_hudong_structure_new'
                                or doc_site in (
                                    'm.baike.com',
                                    'byte.baike.com',
                                    'www.baike.com',
                                    'm.baike.so.com',
                                    'baike.so.com',
                                    'baike.sogou.com'
                                )
                            )
                            and in_tfs in ('HW', 'OP')
                            AND in_ogs in (1, 2)
                            and source in (
                                'client',
                                'input',
                                'sug',
                                'search_history'
                            )
                            and search_subtab_name = 'synthesis'
                        GROUP by
                            doc_title
                    ) as d
                where
                    doc_title NOT IN ('', '[baike_hudong_structure_new]')
                GROUP by
                    d.doc_title
                order by
                    click_pv desc
                limit
                    100000000
            ) t ON t.original_title = wiki_doc.doc_title
        WHERE
            (
                (
                    original_attribute = ''
                    AND sequence = 1
                )
                OR (
                    original_attribute != ''
                    AND original_attribute = attribute
                )
            )
            AND status IN(1, 2, 3, 4)
            AND merge_to = 0
            AND date = {date}
        GROUP BY
            id
    ) tt
    INNER JOIN toutiao_baike.wiki_doc ON wiki_doc.id = tt.id
WHERE
    date = {date} and click_pv>0""")

    print(df.count(),"===============")

    spark.stop()


if __name__ == '__main__':
    run()

