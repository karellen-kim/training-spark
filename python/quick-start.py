# PySpark 라이브러리 임포트
from pyspark.sql import SparkSession

# SparkSession 생성
#spark = SparkSession.builder.appName("Example").getOrCreate()
# SparkSession 생성
spark = SparkSession.builder.master("local[*]").getOrCreate().stop()

# 데이터 로드
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Dave", 40)]
df = spark.createDataFrame(data, ["Name", "Age"])

# 데이터 처리
df.show()  # 데이터프레임 내용 출력

# 나이가 30 이상인 사람 필터링
filtered_df = df.filter(df["Age"] >= 30)

# 결과 출력
filtered_df.show()

# SparkSession 종료
spark.stop()