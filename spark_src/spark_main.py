import subprocess
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from src.config import Config





def start_spark_process(
        spark_app_name = 'Reddit',
        kafka_topic = Config.get('DEFAULT_KAFKA_TOPIC'),
        check_point_path = '/temp/checkpoint',
        clear_checpoint_path = False
):
    base_docker_exec_command = 'docker exec -it infrastructure-spark-master-1  bash -c '
    pre_bash_command = ' cd '



    spark_app_path = "/temp/scripts/streaming_main.py"

    mongodb_url = f"mongodb://{Config.get('MONGO_HOST')}:{Config.get('MONGO_PORT')}"

    kafka_server = f"{Config.get('KAFKA_INSIDE_HOST')}:{Config.get('KAFKA_INSIDE_PORT')}"       

    mongo_database = Config.get('MONGO_DATABASE')

    clear_checpoint_path_flag = '-ccp' if clear_checpoint_path else ''


    bash_command = f""" python {spark_app_path} -n {spark_app_name} -mu '{mongodb_url}' -ks {kafka_server} -kt {kafka_topic} -md {mongo_database} -cp {check_point_path} {clear_checpoint_path_flag} """
    full_command = f""" {base_docker_exec_command} "{pre_bash_command} && {bash_command}" """



    execute_command(full_command)




def execute_command(bash_command):
    """
    Execute a Bash command and stream the output line by line.
    
    Args:
        bash_command (str): The Bash command to execute.
    """
    # Execute the command
    process = subprocess.Popen(bash_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors='ignore')

    # Stream the output line by line
    for line in process.stdout:
        print(line.strip())  # Strip any leading/trailing whitespace

    # Check if the command was successful
    return_code = process.wait()
    if return_code == 0:
        print("Command executed successfully.")
    else:
        print("Error executing command.")
    




start_spark_process(clear_checpoint_path=True)

# python spark_src\spark_main.py 