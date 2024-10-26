import logging
import logging.handlers
import datetime
import os
import json
import re
from collections import OrderedDict, defaultdict
from pathlib import Path

from splunk.persistconn.application import PersistentServerConnectionApplication

# to do 
"""
deploy this on ds and check if successfully reaching uf 
fix timestamp - date time doesnt have now()
create ability to delete source/app probably in a new endpoint
create ability to update source/app probably in a new endpoint
"""

# this app is targetted to DS -- change the path 



def setup_logger(level):
    logger = logging.getLogger('_rest_process_payload_toconfigs')
    logger.setLevel(level)
    handler = logging.handlers.RotatingFileHandler(os.environ['SPLUNK_HOME']+'/var/log/splunk/_rest_process_payload_toconfigs.log', maxBytes=1000000, backupCount=5)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

logger = setup_logger(logging.DEBUG)

class MyRestHandler(PersistentServerConnectionApplication):
    
    directory_path = Path(os.environ.get('SPLUNK_HOME', '')) / 'etc' / 'deployment-apps'


    def __init__(self, command_line, command_arg):
        super(MyRestHandler, self).__init__()



    def get_values(self, in_string):
        logger.debug(f"Received payload: {in_string}")
        try:
            # Parse the outer JSON
            outer_json = json.loads(in_string)
            logger.debug(f"Parsed outer JSON: {outer_json}")
            
            # Get the payload from the outer JSON and parse it
            payload_json = json.loads(outer_json['payload'])
            logger.debug(f"Parsed payload JSON: {payload_json}")
            
            # Extract the required values
            message = payload_json.get('message', '')
            index_name = payload_json.get('my_index', '')
            my_sourcetype = payload_json.get('my_sourcetype', '')
            my_source = payload_json.get('my_source', {})
            my_host = payload_json.get('my_host', {})
            
            # Extract additional metadata
            additional_metadata = payload_json.get('additional_metadata', {})
            app_name = additional_metadata.get('app_name', '')
            environment = additional_metadata.get('environment', '')
            version = additional_metadata.get('version', '')

            return index_name, message, my_sourcetype, app_name, environment, version, my_source, my_host

        except json.JSONDecodeError as e:
            logger.error(f"JSON decoding error: {str(e)}")
            raise
        except KeyError as e:
            logger.error(f"Missing key in JSON: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_values: {str(e)}")
            raise
    def generate_configs(self,sources, sourcetype, index):
        configs = [
            f"[monitor://{source_value}]\n"
            f"index = {index}\n"
            f"sourcetype = {sourcetype}\n"
            f"disabled = false\n\n"
            for source_key, source_value in sources.items()
        ]
        logger.info(f"Generated {len(configs)} configurations")
        return configs

    def compare_and_update_input_configs(self, local_conf_path, configs, message):
        existing_configs = defaultdict(list)
        if local_conf_path.exists():
            logger.info(f"Reading existing configs from {local_conf_path}")
            
            with local_conf_path.open('r') as f:
                current_stanza = None
                for line in f:
                    line = line.strip()
                    if line.startswith('[monitor://'):
                        current_stanza = line
                    elif current_stanza and line:
                        existing_configs[current_stanza].append(line)
        
        new_configs = {}
        logger.info(f"Comparing existing configs with new configs")
        for config in configs:
            lines = config.strip().split('\n')
            if lines and lines[0].startswith('[monitor://'):
                stanza = lines[0]
                if stanza not in existing_configs:
                    new_configs[stanza] = lines[1:]

        if new_configs:
            logger.info(f"Attempting to write new configs : {local_conf_path}")
            with local_conf_path.open('a') as f:
                # Write the message once, outside the loop
                f.write(f'\n # *** Start of inputs from : {message}\n')
                
                for stanza, lines in new_configs.items():
                    f.write(f"{stanza}\n")
                    for line in lines:
                        f.write(f"{line}\n")
                    f.write('\n')
                
                f.write(f'\n# *** End of inputs from : {message}\n')

        return len(new_configs)

    def write_configs_to_file(self, configs, app_name, message, filename=None):
        if filename is None:
            
            directory = self.directory_path / app_name / 'default'
                
        try:
            
            app_conf_path = directory / 'app.conf'
            local_conf_path = directory / 'inputs.conf'
            
            directory.mkdir(parents=True, exist_ok=True)
            
            is_new_directory = not app_conf_path.exists()
            logger.debug(f"Directory : {directory} | is_new_directory : {is_new_directory}")
            
            
            if is_new_directory:
                logger.info(f"Created new app: {directory}")
                
                
                app_conf_path = os.path.join(directory, 'app.conf')
                # current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")
                
                
                app_conf_content = f"""
#==============================================================================
# {app_name.center(76, '*')}
#==============================================================================
# Created by: Automated Splunk App Configuration
# Author: Vignesh Narendran
# Creation Date: current_time
# 
# Description:
# This file was automatically generated as part of the {app_name} Splunk app.
# It contains initial configuration settings. Don't modify, as it may affect
# the functionality of the app
#
# Warning: This file may be overwritten by future automated updates.
# 
#==============================================================================

"""
                with open(app_conf_path, 'w') as app_conf:
                    app_conf.write(app_conf_content.strip())

                logger.info(f"Created app.conf file: {app_conf_path}")
            else: 
                logger.info(f"App : {app_name} existing in the target {directory}, attempting to compare and add new inputs") 

            logger.debug(f"Path: {local_conf_path} | Configs : {configs}")
            new_configs_len=self.compare_and_update_input_configs(local_conf_path, configs, message)
            action = "Created new" if is_new_directory else "Appended new configurations to"
            logger.info(f"{action} inputs.conf file: {local_conf_path}")
            logger.info(f"Added {new_configs_len} new configurations.")

            
            logger.info(f"Configuration update completed for {app_name}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to write configurations to file: {str(e)}")
            return False

    def compare_and_add_serverclass_whitelist_conf(self, app_name, index, sourcetype, hosts):
        
        serverclass_directory = Path(os.environ.get('SPLUNK_HOME', '')) / 'etc' / 'automated_config_generator_serverclass'
        os.makedirs(serverclass_directory, exist_ok=True)
        serverclass_path = os.path.join(serverclass_directory, 'serverclass.conf')
        serverclass_name = f"{app_name}_{index}_{sourcetype}"

        existing_content = ""
        existing_hosts = set()
        max_whitelist_num = -1
        server_class_exists = False
        app_stanza_start = None
        app_stanza_end = None

        # Read existing content and check for the server class
        if os.path.exists(serverclass_path):

            with open(serverclass_path, 'r') as f:
                existing_content = f.read()
                app_stanza_pattern = re.compile(rf'\[serverClass:{re.escape(serverclass_name)}:app:{re.escape(app_name)}\](.*?)(?=\n\[|\Z)', re.DOTALL)
                match = app_stanza_pattern.search(existing_content)
                if match:
                    logger.info(f"Found existing server class configuration for {serverclass_name}")
                    server_class_exists = True
                    # mark start and end of matched serverclass to append new hosts 
                    app_stanza_start = match.start()
                    app_stanza_end = match.end()
                    whitelist_pattern = re.compile(rf'whitelist\.(\d+)\s*=\s*(\S+)')
                    for match in whitelist_pattern.finditer(match.group(1)):
                        num, host = match.groups()
                        existing_hosts.add(host)
                        max_whitelist_num = max(max_whitelist_num, int(num))

        new_hosts = OrderedDict((hostname, None) for hostname in hosts.values() if hostname not in existing_hosts)

        # current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")
# class doesnt exist, create new
        if not server_class_exists:
            logger.info(f"Creating new server class configuration for {serverclass_name}")
            content = f"""\n # Added on current_time
[serverClass:{serverclass_name}]
restartSplunkWeb = 0
restartSplunkd = 1
stateOnClient = enabled

[serverClass:{serverclass_name}:app:{app_name}]
"""
            for i, hostname in enumerate(new_hosts.keys()):
                content += f"whitelist.{i} = {hostname}\n"

            with open(serverclass_path, 'a') as f:
                f.write(content)
            print(f"Added new server class configuration for {serverclass_name}")
            return True

        elif new_hosts:
            logger.info(f"Adding new hosts to existing server class configuration for {serverclass_name}")
            
            # current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")
            content = f"""# Updated on current_time\n"""
            for i, hostname in enumerate(new_hosts.keys(), start=max_whitelist_num + 1):
                content += f"whitelist.{i} = {hostname}\n"
            # use the end of serverclass pointer found above to append from end
            # start of serverclass reserved for future development.
            updated_content = existing_content[:app_stanza_end] + content + existing_content[app_stanza_end:]
            # logger.debug(f"Updated content : {updated_content}")

            with open(serverclass_path, 'w') as f:
                f.write(updated_content)

            print(f"Added {len(new_hosts)} new hosts to existing server class {serverclass_name}")
            return True

        else:
            print(f"No new hosts to add for server class {serverclass_name}. Configuration unchanged.")
            return False



    def handle(self, in_string):

        return_payload_string = ""
        try:
            logger.info(f"Received payload: {in_string}")
            index_name, message, my_sourcetype, app_name, environment, version, my_source, my_host = self.get_values(in_string)
            # perform all the calls from here, and get its status 
            if app_name:
                return_payload_string += "Payload extract = success | "
                configs = self.generate_configs(my_source, my_sourcetype, index_name)
                if configs:
                    return_payload_string += "Generate config = success | "
                    config_write_status = self.write_configs_to_file(configs, app_name, message)
                    if config_write_status:
                        return_payload_string += "Write config = success | "
                        serverclass_status = self.compare_and_add_serverclass_whitelist_conf(app_name, index_name, my_sourcetype, my_host)
                        if serverclass_status:
                            return_payload_string += "Write serverclass = success | "
                        else:
                            return_payload_string += "Write serverclass = failed | "
                    else:
                        return_payload_string += "Write config = failed | "
                else:
                    return_payload_string += "Generate config = failed | "
            else:
                return_payload_string = "Payload extract = failed | "

   
            return_payload_string += "..."

            return {
                'payload': {
                    "status": "success",
                    "message": "Payload received and logged.",
                    "Script_status": return_payload_string
                },
                'status': 200
            }

        except json.JSONDecodeError:
            logger.error("Invalid JSON format")
            return {
                'payload': {"status": "error", "message": "Invalid JSON format."},
                'status': 400
            }

        except Exception as e:
            logger.error(f"Error processing request: {e}")
            return {
                'payload': {"status": "error", "message": str(e)},
                'status': 500
            }


    def done(self):
        """
        Called after the request completes (optional).
        """
        pass

