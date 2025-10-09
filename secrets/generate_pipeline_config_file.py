import os
import sys
import argparse

# Squad abbreviations mapping
SQUADS_REFERENCE = {
    'Consignado Privado': {'abb': 'con', 'snake_case': 'consignado_privado'},
    'Consignado Publico': {'abb': 'emp', 'snake_case': 'consignado_publico'},
    'Martech': {'abb': 'mkt', 'snake_case': 'martech'},
}

def parse_horario_config(horario_config):
    """Parse time configuration string: start_hour,start_minute,end_hour,frequency"""
    parts = horario_config.split(',')
    if len(parts) != 4:
        raise ValueError("horario_config must have 4 values: start_hour,start_minute,end_hour,frequency")
    
    return {
        'horario_inicio': parts[0].strip(),
        'minuto_inicio': parts[1].strip(),
        'horario_fim': parts[2].strip() if parts[2].strip() else None,
        'frequencia_atualizacao_hora': int(parts[3].strip()) if parts[3].strip() else 1
    }

def parse_cluster_config(cluster_config):
    """Parse cluster configuration string: worker_type,driver_type"""
    parts = cluster_config.split(',')
    if len(parts) != 2:
        raise ValueError("cluster_config must have 2 values: worker_type,driver_type")
    
    return {
        'worker_type': parts[0].strip(),
        'driver_type': parts[1].strip()
    }

# def generate_cron_expression(rotina, horario_inicio, minuto_inicio, horario_fim, frequencia_atualizacao_hora, dias_da_semana):
#     """Generate Quartz cron expression based on inputs."""
#     dias_mapping = {
#         'domingo': '1', 'segunda': '2', 'terca': '3', 
#         'quarta': '4', 'quinta': '5', 'sexta': '6', 'sabado': '7'
#     }
#     dias_cron = ','.join([dias_mapping[dia] for dia in dias_da_semana])
    
#     if rotina == 'diaria':
#         return f"0 {minuto_inicio} {horario_inicio} ? * {dias_cron}"
#     else:  # hora
#         horas = list(range(int(horario_inicio), int(horario_fim) + 1, frequencia_atualizacao_hora))
#         horas_cron = ','.join(map(str, horas))
#         return f"0 {minuto_inicio} {horas_cron} ? * {dias_cron}"


def generate_cron_expression(rotina, horario_inicio, minuto_inicio, horario_fim, frequencia_atualizacao_hora, dias_da_semana):
    """Generate Quartz cron expression based on inputs."""
    
    # Convert day names to Quartz day numbers (1=Sunday, 2=Monday, ..., 7=Saturday)
    dias_mapping = {
        'domingo': '1', 'segunda': '2', 'terca': '3', 
        'quarta': '4', 'quinta': '5', 'sexta': '6', 'sabado': '7'
    }
    
    # Parse the dias_da_semana string
    dias_list = [d.strip() for d in dias_da_semana]
    dias_numbers = [dias_mapping[dia] for dia in dias_list]
    dias_cron = ''
    if len(dias_numbers) < 7:
        dias_cron = ','.join(dias_numbers)
    
    if rotina == 'diaria':
        # Daily routine: runs once per day at specified time
        return f"0 {minuto_inicio} {horario_inicio} ? * {dias_cron}"
    else:  # rotina == 'hora'
        # Hourly routine: use range syntax for hours
        freq = int(frequencia_atualizacao_hora)
        inicio = int(horario_inicio)
        fim = int(horario_fim)
        
        # Use Quartz range syntax: start-end/frequency
        horas_cron = f"{inicio}-{fim}/{freq}"
        
        return f"0 {minuto_inicio} {horas_cron} ? * {dias_cron}"

def generate_clusters_section(worker_type, driver_type):
    """Generate clusters section for non-serverless pipelines."""
    return f'''
      clusters:
        - label: "default"
          aws_attributes:
            instance_profile_arn: "arn:aws:iam::123:instance-profile/abc"
          node_type_id: "{worker_type}"
          driver_node_type_id: "{driver_type}"
          autoscale:
            min_workers: 1
            max_workers: 6
        - label: "maintenance"
          aws_attributes:
            instance_profile_arn: "arn:aws:iam::123:instance-profile/abc"
            node_type_id: "{worker_type}"
            driver_node_type_id: "{driver_type}"
'''

def generate_pipeline_yml(squad, table_name, rotina, horario_inicio, minuto_inicio, 
                          horario_fim, frequencia_atualizacao_hora, dias_da_semana, 
                          serverless, worker_type, driver_type):
    """Generate the complete pipeline YAML configuration."""
    
    # Generate variables
    squad_abbreviation = SQUADS_REFERENCE[squad]['abb']
    squad_snake_case = SQUADS_REFERENCE[squad]['snake_case']
    
    # Generate cron expression
    cron_expression = generate_cron_expression(
        rotina, horario_inicio, minuto_inicio, horario_fim, 
        frequencia_atualizacao_hora, dias_da_semana
    )
    
    # Generate names
    horario_execucao = f'{horario_inicio}h' if rotina == 'diaria' else f'{horario_inicio}h_{horario_fim}h'
    pipeline_name = f'{squad_abbreviation}_pipe_rotina_{rotina}_{horario_execucao}'
    job_name = f'{squad_abbreviation}_job_rotina_{rotina}_{horario_execucao}'
    notebook_path = f'/Users/svc.databricksprd@meutudo.app/mt-datateam-databricks/meutudo/materialized/{squad_snake_case}/note_processa_{table_name}'
    
    # Generate clusters section if not serverless
    clusters_section = ""
    if not serverless:
        clusters_section = generate_clusters_section(worker_type, driver_type)
    
    # Generate template
    template = f'''resources: 
  pipelines:
    {pipeline_name}:
      name: "{pipeline_name}"

      permissions:
        - level: IS_OWNER
          user_name: "dataeng.databricks@meutudo.app"
        - level: CAN_MANAGE
          user_name: "svc.databricksprd@meutudo.app"
        - level: CAN_RUN
          group_name: "data-engineers"
        - level: CAN_RUN
          group_name: "data-leaders"
        - level: CAN_RUN
          group_name: "data-seniors"
        - level: CAN_VIEW
          group_name: "data-analytics"
        - level: CAN_VIEW
          group_name: "data-science"
        - level: CAN_VIEW
          group_name: "data-partner"
      
      serverless: {str(serverless).lower()}
      photon: false
      channel: "current"
      continuous: false
      catalog: "meutudo"
      schema: "gold"
{clusters_section}
      libraries:
        - notebook:
            path: "{notebook_path}"
      
      tags:
        Squad: "{squad}"
        Workflow: "Rotina {rotina}"
        Tipo: "MATERIALIZADA"
        Pipeline: "{pipeline_name}"
      
      configuration:
        "pipelines.unityCatalog.allowSchemaOverride": "true"

  jobs:
    {job_name}:
      name: "{job_name}"
      
      schedule:
        quartz_cron_expression: "{cron_expression}"
        timezone_id: "America/Sao_Paulo"
        pause_status: "UNPAUSED"

      webhook_notifications:
        on_failure:
          - id: id
            
      timeout_seconds: 3600
      max_concurrent_runs: 1
      
      permissions:
        - level: IS_OWNER
          user_name: "dataeng.databricks@meutudo.app"
        - level: CAN_MANAGE
          user_name: "svc.databricksprd@meutudo.app"
        - level: CAN_MANAGE_RUN
          group_name: "data-engineers"
        - level: CAN_MANAGE_RUN
          group_name: "data-leaders"
        - level: CAN_MANAGE_RUN
          group_name: "data-seniors"
        - level: CAN_VIEW
          group_name: "data-analytics"
        - level: CAN_VIEW
          group_name: "data-science"
        - level: CAN_VIEW
          group_name: "data-partner"

      run_as:
        user_name: "dataeng.databricks@meutudo.app"
      
      tasks:
        - task_key: trigger_pipeline
          pipeline_task:
            pipeline_id: ${{resources.pipelines.{pipeline_name}.id}}
            full_refresh: false
      
      tags:
        Squad: "{squad}"
        Workflow: "Rotina {rotina}"
        Tipo: "MATERIALIZADA"
        Job: "{job_name}"
'''
    
    return template, pipeline_name, squad

def main():
    parser = argparse.ArgumentParser(description='Generate Databricks pipeline YAML configuration')
    parser.add_argument('--squad', required=True, choices=['Consignado Privado', 'Consignado Publico', 'Martech'])
    parser.add_argument('--table-name', required=True)
    parser.add_argument('--rotina', required=True, choices=['diaria', 'hora'])
    parser.add_argument('--horario-config', required=True, 
                        help='Time config: start_hour,start_minute,end_hour,frequency (e.g., 7,15,20,1)')
    parser.add_argument('--dias-da-semana', required=True, help='Comma-separated list of days')
    parser.add_argument('--serverless', required=True, choices=['true', 'false'])
    parser.add_argument('--cluster-config', default='r6i.4xlarge,r6i.xlarge',
                        help='Cluster config: worker_type,driver_type (e.g., r6i.4xlarge,r6i.xlarge)')
    
    args = parser.parse_args()
    
    # Parse time configuration
    horario_params = parse_horario_config(args.horario_config)
    
    # Parse cluster configuration
    cluster_params = parse_cluster_config(args.cluster_config)
    
    # Parse days of week
    dias_da_semana = [dia.strip() for dia in args.dias_da_semana.split(',')]
    
    # Convert serverless to boolean
    serverless = args.serverless == 'true'
    
    # Generate the YAML
    template, pipeline_name, squad = generate_pipeline_yml(
        args.squad,
        args.table_name,
        args.rotina,
        horario_params['horario_inicio'],
        horario_params['minuto_inicio'],
        horario_params['horario_fim'],
        horario_params['frequencia_atualizacao_hora'],
        dias_da_semana,
        serverless,
        cluster_params['worker_type'],
        cluster_params['driver_type']
    )
    
    # Create directory if it doesn't exist
    directory = f"pipelines_config/{SQUADS_REFERENCE[squad]['snake_case']}"
    os.makedirs(directory, exist_ok=True)
    
    # Write file
    file_path = f"{directory}/{pipeline_name}.yml"
    with open(file_path, 'w') as f:
        f.write(template)
    
    # Output for GitHub Actions
    print(f"FILE_PATH={file_path}")
    print(f"PIPELINE_NAME={pipeline_name}")
    print(f"SQUAD={squad}")
    
    print(f"\n✅ Generated pipeline configuration at: {file_path}", file=sys.stderr)

if __name__ == "__main__":
    main()
