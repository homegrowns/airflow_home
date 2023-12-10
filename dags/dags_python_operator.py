
import datetime
import pendulum
import random
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    # 직관적으로 수정할 덱을 빨리찾기위해 덱아이디(dag_id)와 파이썬 파일명 일치시킨다.  dag_id
    dag_id="dags_python_operator",
            # 분,시,일,월,요일
    schedule="30 6 * * *",
            # UTC(세계표준시) 9시간 더늦게돈다 그래서 한국시간대로 맞춘다   
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
            # 현재가 12/06이면 catchup=True시 2021 1/1부터 현재 날짜까지 빈기간을 모두 한꺼번에 돌리게 된다
            # 덱을 어떻게 만들었냐에 따라서 문제가 될 수 있다.
    catchup=False,
            # dagrun_timeout => 덱이 60분 이상돌면 실패
    dagrun_timeout=datetime.timedelta(minutes=60),
    # params => 테스크들에 공통적으로 넘겨줄 파라미터
    # params={"example_key": "example_value"},
) as dag:
    def select_fruit():
        fruit = ['APPLE', 'BANANA', 'ORANGE', 'AVOCADO']
        rand_int = random.randint(0,3)
        print(fruit[rand_int])