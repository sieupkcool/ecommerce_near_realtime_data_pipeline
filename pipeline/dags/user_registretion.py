from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor



def check_cities_exist_callable():
    from ecommerce.models.city import City
    city_instance = City()
    exists = city_instance.has_cities()
    if exists:
        print("Cities found. Proceeding...")
        return True
    else:
        print("Waiting for cities to be created...")
        return False

def user_info(num_users=1, execution_date_str=None):
    from ecommerce.models.user import User
    instance = User()
    instance.generate_fake_users(num_users=num_users, execution_date_str=execution_date_str)


def user_address():
    from ecommerce.models.address import Address
    instance = Address()
    instance.generate_addresses()


def assign_role():
    from ecommerce.models.role_user import RoleUser
    instance = RoleUser()
    instance.assign_roles_to_users()


# def insert_role():
#     instance = Role()
#     instance.generate_roles()


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG('ecommerce_user_registration',
        start_date=datetime(2025, 10, 30),
        schedule='@daily',
        default_args=default_args, 
        catchup=True,
        max_active_runs=1
) as dag:

    # insert_role_to_db = PythonOperator(
    #     task_id='insert_role_to_db',
    #     python_callable=insert_role
    # )

    wait_for_cities = PythonSensor(
        task_id="wait_for_cities",
        python_callable=check_cities_exist_callable,
        poke_interval=30, 
        timeout=900,      
        mode="reschedule"
    )

    generate_user_info = PythonOperator(
        task_id='generate_user_info',
        python_callable=user_info,
        op_args=[100, '{{ ds }}'] 
    )

    assign_role_to_user = PythonOperator(
        task_id='assign_role_to_user',
        python_callable=assign_role
    )

    generate_user_address = PythonOperator(
        task_id='generate_user_address',
        python_callable=user_address
    )

# insert_role_to_db >> generate_user_info >> assign_role_to_user >> generate_user_address

wait_for_cities >> generate_user_info >> assign_role_to_user >> generate_user_address
