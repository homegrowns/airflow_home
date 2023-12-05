
import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    # 직관적으로 수정할 덱을 빨리찾기위해 덱아이디(dag_id)와 파이썬 파일명 일치시킨다.  dag_id
    dag_id="dags_bash_operator",
            # 분,시,일,월,요일
    schedule="0 0 * * *",
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
    # 객체명과 task_id는 일치시킨다.
    bsah_t1 = BashOperator(
        task_id="bsah_t1",
        bash_command="echo whoami",
    )

    bsah_t2 = BashOperator(
        task_id="bsah_t2",
        bash_command="echo $HOSTNAME",
    )

    bsah_t3 = BashOperator(
        task_id="bsah_t3",
        bash_command="echo ang ki mo chi",
    )

    bsah_t1 >> bsah_t2 >> bsah_t3


#     # [START howto_operator_bash]
#     run_this = BashOperator(
#         task_id="run_after_loop",
#         bash_command="echo 1",
#     )
#     # [END howto_operator_bash]

#     run_this >> run_this_last

#     for i in range(3):
#         task = BashOperator(
#             task_id=f"runme_{i}",
#             bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
#         )
#         task >> run_this

#     # [START howto_operator_bash_template]
#     also_run_this = BashOperator(
#         task_id="also_run_this",
#         bash_command='echo "ti_key={{ task_instance_key_str }}"',
#     )
#     # [END howto_operator_bash_template]
#     also_run_this >> run_this_last

# # [START howto_operator_bash_skip]
# this_will_skip = BashOperator(
#     task_id="this_will_skip",
#     bash_command='echo "hello world"; exit 99;',
#     dag=dag,
# )
# # [END howto_operator_bash_skip]
# this_will_skip >> run_this_last

# if __name__ == "__main__":
#     dag.test()