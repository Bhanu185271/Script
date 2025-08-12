"""
Module to define all available routes
"""
from flask import jsonify, Response
from Api import DatabricksToken as dt
from Api import DataConsumption as dc
from Api import app
from databricks import sql		
from flask import Flask, request, jsonify
from flask_api import status

import hashlib, json, logging, traceback
import pandas as pd
import os
import re
import json
# from flask_restplus import Resource
from cachetools import TTLCache
import datetime
import copy
from diskcache import Cache
import shutil
# route_cache = TTLCache(maxsize=1000, ttl=3600)
dir_path = os.path.dirname(os.path.realpath(__file__))
route_cache = Cache("Cache")

@app.route('/home')
# class Home(Resource):
def get():
    """
    return description about API
    """
    # print(source)
    desc = "Data API is used to retreive analytics ready data stored in azure databricks. The output of data is in standard JSON."
    
    return jsonify(desc)

@app.route('/<source>/v2/<project>/<market>')
# class GetData(Resource):
def get1(project, source, market):
    """
    Returns requested data after apilying authorization schemes
    """
    project = project.lower()
    source = source.lower()
    market = market
    # product = product.lower()
    #table = table.lower()

    try:
        #     connection_data = dc.config_data()
        # except Exception:
        #     return jsonify('Project name is not correct')
        
        param_req = dict(request.args)
        print('Request Headers ',request.headers)
        client_id =  request.headers.get('Clientid')
        client_secret =  request.headers.get('Clientsecret')
        # server_hostname = connection_data['server-hostname']
        # http_path = connection_data['http-path']
        # connection_string = connection_data['container-string']
        # tenantId = connection_data['tenantId']
        # subscription_id = connection_data['subscription_id']
        # resource_group = connection_data['resource_group']
        # databricks_workspace = connection_data['databricks_workspace']
        # dbricks_location = connection_data['dbricks_location']
        
        # try:
        # try:
        #     databricks_token = dt.DatabricksToken(client_id,client_secret,tenantId, subscription_id,
        #                                             resource_group, databricks_workspace, dbricks_location)
        #     response = databricks_token.db_access_token()
        #     dict_content = json.loads(response.content.decode('utf-8'))
        #     db_token = dict_content.get('token_value')
        # except:
        #     return jsonify(response)
        db_token = os.environ['db_token']
        http_path = os.environ['adb_sqlwarehouse_path']
        server_hostname = os.environ['server_hostname']
        databricks_data = dc.DatabricksData(db_token, server_hostname,
                                            http_path, project, source, market)
        param = {}
        sorted_lower_keys = sorted(param_req.keys(), key=lambda x: x.lower())
        # Create a new dictionary with lowercase keys
        param = {key.lower(): param_req[key] for key in sorted_lower_keys}
        cache_ind = "None"
        final_string = "None"
        cache_len = "None"
        if "page_id" in param:
            param_without_page_id = copy.deepcopy(param)
            del param_without_page_id["page_id"]
            table_filter_without_page_id = databricks_data.get_table_filters(param_without_page_id)
            if str(type(table_filter_without_page_id)) == "<class 'list'>":
                values_as_string = '_'.join(map(str, param_without_page_id.items()))
                final_string = source+'_'+project+'_'+market+'_'+values_as_string
                print(final_string)
                page_id_list = route_cache.get(final_string)
                try:
                    cache_len = len(page_id_list)
                except:
                    cache_len = "None Fetched"

                if page_id_list is not None:
                    print("Fetched Cached data")
                    cache_ind = 'Y'
                else:
                    print("Getting New data")
                    cache_ind = 'N'
                    page_id_list = databricks_data.get_page_data(table_filter_without_page_id)
                    print(page_id_list)
                    route_cache[final_string] = page_id_list

                if len(page_id_list) > 0:
                    for page in range(len(page_id_list)):
                        current_page_id, current_page_count = next(iter(page_id_list[page].items()))
                        if current_page_id == param["page_id"]:
                            if page == len(page_id_list)-1:
                                next_page_id = "None"
                                next_page_count = "None"
                            else:
                                next_page_id, next_page_count = next(iter(page_id_list[page+1].items()))
                            break
                        else:
                            continue
                else:
                    return jsonify("Data does not exist for given parameters")
            else:
                return jsonify(table_filter_without_page_id)
        else:
            values_as_string = '_'.join(map(str, param.items()))
            final_string = source+'_'+project+'_'+market+'_'+values_as_string
            page_id_list = route_cache.get(final_string)
            try:
                cache_len = len(page_id_list)
            except:
                cache_len = "None Fetched"

            print(final_string)

            if page_id_list is not None:
                cache_ind = 'Y'
                print("Fetched Cached data")
                print(page_id_list)
            else:
                print("Getting New data")
                cache_ind = 'N'
                table_filter_without_page_id = databricks_data.get_table_filters(param)
                if str(type(table_filter_without_page_id)) == "<class 'list'>":
                    page_id_list = databricks_data.get_page_data(table_filter_without_page_id)
                    print(page_id_list)
                    route_cache[final_string] = page_id_list
                else:
                    return jsonify(table_filter_without_page_id)
            if len(page_id_list) > 1:

                current_page_id, current_page_count = next(iter(page_id_list[0].items()))
                next_page_id, next_page_count = next(iter(page_id_list[1].items()))
                param["page_id"] = current_page_id
            elif len(page_id_list) == 1:

                current_page_id, current_page_count = next(iter(page_id_list[0].items()))
                next_page_id = "None"
                next_page_count = "None"
                param["page_id"] = current_page_id
            else:
                return jsonify("Data does not exist for given parameters")
        
        print("param from API console: ",param)
        table_filter = databricks_data.get_table_filters(param)

        if str(type(table_filter)) == "<class 'list'>":

            qurried_data = databricks_data.get_data_databricks(table_filter)
            print("table_filter: ",table_filter)
            final_data = list()
            for record in qurried_data:
                record.pop('PAGE_ID', None)
                final_data.append(record)
            
            # if param["page_id"]:
            #     param["page_id"] = str(int(param["page_id"])+1)
            #     next_page_table_filter = databricks_data.get_table_filters(param)
            #     print("next_page_table_filter: ",next_page_table_filter)

            #     next_page_data = databricks_data.get_data_next_page(next_page_table_filter, table_filter, param["page_id"])
            #     qurried_data = {"page_details": next_page_data, "records": qurried_data}
                
            # if str(type(table_filter)) == "<class 'list'>":
            # qurried_data = {"page_details": {'current_page_id': current_page_id, 
            # 'current_page_count': current_page_count, 
            # 'next_page_id': next_page_id, 
            # 'next_page_count': next_page_count, 
            # 'cache_ind': cache_ind, 'final_string': final_string,
            # 'cache_len': cache_len}, "records": qurried_data}
            qurried_data = {"page_details": {'current_page_id': current_page_id, 
            'current_page_count': current_page_count, 
            'next_page_id': next_page_id, 
            'next_page_count': next_page_count}, "records": final_data}
            return jsonify(qurried_data)
            
        else:
            return jsonify(table_filter)
    
    except Exception as err:
        return jsonify('Error connecting to Databricks due to '+str(err))

@app.route('/cache_burst')
# class CacheBurst(Resource):
def get2():
    try:
        route_cache.clear()
        # shutil.rmtree(r'Cache')
        return jsonify("Cache Deleted")
    except Exception as err:
        return jsonify('Error deleting cache '+str(err))

@app.route('/<source>/v1/<project>/<market>')
# class GetDataOriginal(Resource):
    # def get(project, source, market):
    #     desc = "Test Route"
        
    #     return jsonify(desc)
def get3(project, source, market):
    """
    Returns requested data after apilying authorization schemes
    """
    project = project.lower()
    source = source.lower()
    market = market
    
    param = dict(request.args)
    print('Request Headers ',request.headers)
    client_id =  request.headers.get('Clientid')
    client_secret =  request.headers.get('Clientsecret')
    
    try:
        db_token = os.environ['db_token']
        http_path = os.environ['adb_sqlwarehouse_path']
        server_hostname = os.environ['server_hostname']
        
        
        databricks_data = dc.DatabricksData(db_token, server_hostname, http_path, project, source, market)
        print("param from API console: ",param)
        table_filter = databricks_data.get_table_filters(param)
        print("table_filter: ",table_filter)

        if str(type(table_filter)) == "<class 'list'>":
            final_response = databricks_data.get_data_databricks(table_filter)
            return jsonify(final_response)
            
        else:
            return jsonify(table_filter)
    
    except Exception as err:
        return jsonify('Error connecting to Databricks due to '+str(err))
        
        
'''
def validate_prompt_request(data):
    try:
        required_keys = [
            "insight_type", "prompt_template", "prompt_params_list",
            "llm_model_name", "temperature", "max_tokens", "verbose", "chunking_params"
        ]
        for k in required_keys:
            if k not in data:
                return False, f"Missing required key: {k}"

        prompt_template = data["prompt_template"]
        if not prompt_template.strip():
            return False, "prompt_template cannot be empty"
        if "{context}" not in prompt_template:
            return False, "prompt_template must contain '{context}'"

        try:
            prompt_params = json.loads(data["prompt_params_list"])
        except json.JSONDecodeError:
            return False, "Invalid JSON in prompt_params_list"
        if "variables" not in prompt_params or not isinstance(prompt_params["variables"], list):
            return False, "'prompt_params_list' must contain a 'variables' list"

        missing_vars = [v for v in prompt_params["variables"] if '{' + v + '}' not in prompt_template]
        if missing_vars:
            return False, f"Missing variables in prompt_template: {', '.join(missing_vars)}"

        try:
            temperature = float(data["temperature"])
            if not (0.0 <= temperature <= 1.0):
                return False, "temperature must be between 0.0 and 1.0"
        except ValueError:
            return False, "temperature must be a float"

        try:
            max_tokens = int(data["max_tokens"])
        except ValueError:
            return False, "max_tokens must be an integer"

        verbose_raw = data["verbose"]
        if isinstance(verbose_raw, bool):
            verbose = verbose_raw
        elif isinstance(verbose_raw, str):
            if verbose_raw.lower() in ["true", "1"]:
                verbose = True
            elif verbose_raw.lower() in ["false", "0"]:
                verbose = False
            else:
                return False, "verbose must be a boolean"
        else:
            return False, "verbose must be a boolean"

        try:
            chunking = json.loads(data["chunking_params"])
        except json.JSONDecodeError:
            return False, "Invalid JSON in chunking_params"
        if not isinstance(chunking, dict):
            return False, "chunking_params must be a JSON object"

        if "chunk_abs" in chunking and "chunk_ratio" in chunking:
            return False, "Provide either chunk_abs or chunk_ratio, not both"
        if "chunk_abs" not in chunking and "chunk_ratio" not in chunking:
            return False, "Must provide either chunk_abs or chunk_ratio"
        if "chunk_abs" in chunking and not isinstance(chunking["chunk_abs"], int):
            return False, "chunk_abs must be an integer"
        if "chunk_ratio" in chunking and not isinstance(chunking["chunk_ratio"], float):
            return False, "chunk_ratio must be a float"
        if "overlap" not in chunking or not isinstance(chunking["overlap"], int):
            return False, "'overlap' is required and must be an integer"
        if "chunk_agg_delim" in chunking and not isinstance(chunking.get("chunk_agg_delim", ""), str):
            return False, "'chunk_agg_delim' must be a string"

        return True, {
            "insight_type": data["insight_type"],
            "prompt_template": prompt_template,
            "prompt_params": prompt_params,
            "llm_model_name": data["llm_model_name"],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "verbose": verbose,
            "chunking": chunking
        }

    except Exception as e:
        return False, str(e)'''
        
import json

def validate_prompt_request(data):
    try:
        required_keys = [
            "insight_type", "prompt_template", "prompt_params_list",
            "llm_model_name", "temperature", "max_tokens", "verbose", "chunking_params"
        ]
        print("Input data keys:", data.keys())

        for k in required_keys:
            if k not in data:
                print(f"Missing required key: {k}")
                return False, f"Missing required key: {k}"

        prompt_template = data["prompt_template"]
        print("prompt_template:", repr(prompt_template))
        if not prompt_template.strip():
            print("prompt_template is empty")
            return False, "prompt_template cannot be empty"
        if "{context}" not in prompt_template:
            print("prompt_template missing '{context}'")
            return False, "prompt_template must contain '{context}'"

        try:
            prompt_params = json.loads(data["prompt_params_list"])
            print("prompt_params:", prompt_params)
        except json.JSONDecodeError:
            print("Invalid JSON in prompt_params_list")
            return False, "Invalid JSON in prompt_params_list"

        if "variables" not in prompt_params or not isinstance(prompt_params["variables"], list):
            print("'prompt_params_list' missing 'variables' list")
            return False, "'prompt_params_list' must contain a 'variables' list"

        missing_vars = [v for v in prompt_params["variables"] if '{' + v + '}' not in prompt_template]
        print("missing_vars in prompt_template:", missing_vars)
        if missing_vars:
            return False, f"Missing variables in prompt_template: {', '.join(missing_vars)}"

        try:
            temperature = float(data["temperature"])
            print("temperature:", temperature)
            if not (0.0 <= temperature <= 1.0):
                print("temperature out of range")
                return False, "temperature must be between 0.0 and 1.0"
        except ValueError:
            print("temperature is not a float")
            return False, "temperature must be a float"

        try:
            max_tokens = int(data["max_tokens"])
            print("max_tokens:", max_tokens)
        except ValueError:
            print("max_tokens is not an integer")
            return False, "max_tokens must be an integer"

        verbose_raw = data["verbose"]
        print("verbose_raw:", verbose_raw)
        if isinstance(verbose_raw, bool):
            verbose = verbose_raw
        elif isinstance(verbose_raw, str):
            if verbose_raw.lower() in ["true", "1"]:
                verbose = True
            elif verbose_raw.lower() in ["false", "0"]:
                verbose = False
            else:
                print("verbose string invalid")
                return False, "verbose must be a boolean"
        else:
            print("verbose invalid type")
            return False, "verbose must be a boolean"
        print("verbose:", verbose)

        try:
            chunking = json.loads(data["chunking_params"])
            print("chunking:", chunking)
        except json.JSONDecodeError:
            print("Invalid JSON in chunking_params")
            return False, "Invalid JSON in chunking_params"
        if not isinstance(chunking, dict):
            print("chunking_params is not a dict")
            return False, "chunking_params must be a JSON object"

        if "chunk_abs" in chunking and "chunk_ratio" in chunking:
            print("Both chunk_abs and chunk_ratio provided")
            return False, "Provide either chunk_abs or chunk_ratio, not both"
        if "chunk_abs" not in chunking and "chunk_ratio" not in chunking:
            print("Neither chunk_abs nor chunk_ratio provided")
            return False, "Must provide either chunk_abs or chunk_ratio"
        if "chunk_abs" in chunking and not isinstance(chunking["chunk_abs"], int):
            print("chunk_abs is not an int")
            return False, "chunk_abs must be an integer"
        if "chunk_ratio" in chunking and not isinstance(chunking["chunk_ratio"], float):
            print("chunk_ratio is not a float")
            return False, "chunk_ratio must be a float"
        if "overlap" not in chunking or not isinstance(chunking["overlap"], int):
            print("overlap missing or not int")
            return False, "'overlap' is required and must be an integer"
        if "chunk_agg_delim" in chunking and not isinstance(chunking.get("chunk_agg_delim", ""), str):
            print("chunk_agg_delim not a string")
            return False, "'chunk_agg_delim' must be a string"

        print("Validation successful. Returning validated data:")
        validated_data = {
            "insight_type": data["insight_type"],
            "prompt_template": prompt_template,
            "prompt_params": prompt_params,
            "llm_model_name": data["llm_model_name"],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "verbose": verbose,
            "chunking": chunking
        }
        for k, v in validated_data.items():
            print(f"  {k}: {v}")
        return True, validated_data

    except Exception as e:
        print("Exception during validation:", str(e))
        return False, str(e)
'''
@app.route('/update_prompt_template', methods=['POST'])
def update_prompt_template():
    try:
        data = request.get_json(force=True)
        is_valid, validated = validate_prompt_request(data)
        if not is_valid:
            return jsonify({"error": validated}), status.HTTP_400_BAD_REQUEST

        username = request.headers.get("Username", "system")
        insight_type = validated.get("insight_type", "")
        prompt_template = validated.get("prompt_template", "")
        prompt_params = validated.get("prompt_params", {})
        llm_model_name = validated.get("llm_model_name", "")

        temperature_raw = data.get("temperature")
        if temperature_raw is None:
            return jsonify({"error": "Missing 'temperature' in request"}), 400
        try:
            temperature = float(temperature_raw)
        except (TypeError, ValueError):
            return jsonify({"error": "'temperature' must be a float"}), 400

        max_tokens_raw = data.get("max_tokens")
        if max_tokens_raw is None:
            return jsonify({"error": "Missing 'max_tokens' in request"}), 400
        try:
            max_tokens = int(max_tokens_raw)
        except (TypeError, ValueError):
            return jsonify({"error": "'max_tokens' must be an integer"}), 400

        verbose_raw = validated.get("verbose")
        if verbose_raw is None:
            verbose = False
        else:
            verbose = bool(verbose_raw)

        chunking = validated.get("chunking", {})

        # PRINT all variables for debugging
        print("=== Debug: Variables ===")
        print(f"username: {username}")
        print(f"insight_type: {insight_type}")
        print(f"prompt_template: {prompt_template}")
        print(f"prompt_params: {prompt_params}")
        print(f"llm_model_name: {llm_model_name}")
        print(f"temperature: {temperature}")
        print(f"max_tokens: {max_tokens}")
        print(f"verbose: {verbose}")
        print(f"chunking: {chunking}")

        # Get latest prompt_model_id safely
        query = """
            SELECT COALESCE(MAX(CAST(prompt_model_id AS INT)), 0) + 1 AS model_id
            FROM hive_metastore.fieldforce_navigator_deployment.llm_prompt_model_master_test
        """
        print(f"query: {query}")
        df = execute_query(query)
        print(f"df: {df}")
        model_id_raw = df.iloc[0].get("model_id") if not df.empty else 1
        print(f"model_id_raw: {model_id_raw}")
        
        try:
            model_id = int(model_id_raw)
        except (ValueError, TypeError):
            model_id = 1

        prompt_hash = hashlib.sha256(prompt_template.encode()).hexdigest()
        entry_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        insert_query = f"""
        INSERT INTO hive_metastore.fieldforce_navigator_deployment.llm_prompt_model_master_test
        VALUES (
            {model_id},
            '{insight_type.replace("'", "''")}',
            '{prompt_template.replace("'", "''")}',
            '{json.dumps(prompt_params).replace("'", "''")}',
            '{prompt_hash}',
            '{llm_model_name.replace("'", "''")}',
            {temperature},
            {max_tokens},
            {str(verbose).lower()},
            timestamp('{entry_time}'),
            '{json.dumps(chunking).replace("'", "''")}',
            '{username.replace("'", "''")}'
        )
        """
        execute_non_query(insert_query)

        return jsonify({"message": f"Prompt inserted with ID {model_id}"}), 200

    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({"error": f"Server error: {str(e)}"}), status.HTTP_500_INTERNAL_SERVER_ERROR


@app.route('/update_prompt_template', methods=['POST'])
def update_prompt_template():
    try:
        data = request.get_json(force=True)
        is_valid, validated = validate_prompt_request(data)
        if not is_valid:
            return jsonify({"error": validated}), status.HTTP_400_BAD_REQUEST

        username = request.headers.get("Username", "system")
        insight_type = validated.get("insight_type", "")
        prompt_template = validated.get("prompt_template", "")
        prompt_params = validated.get("prompt_params", {})
        llm_model_name = validated.get("llm_model_name", "")
        temperature_raw = validated.get("temperature")
        temperature = float(temperature_raw) if temperature_raw is not None else 0.0

        max_tokens_raw = validated.get("max_tokens")
        max_tokens = int(max_tokens_raw) if max_tokens_raw is not None else 0
        temperature_raw = data.get("temperature")
        if temperature_raw is None:
            return jsonify({"error": "Missing 'temperature' in request"}), 400
        try:
            temperature = float(temperature_raw)
        except (TypeError, ValueError):
            return jsonify({"error": "'temperature' must be a float"}), 400

        max_tokens_raw = data.get("max_tokens")
        if max_tokens_raw is None:
            return jsonify({"error": "Missing 'max_tokens' in request"}), 400
        try:
            max_tokens = int(max_tokens_raw)
        except (TypeError, ValueError):
            return jsonify({"error": "'max_tokens' must be an integer"}), 400

        verbose_raw = validated.get("verbose")
        # verbose can be bool or str; coerce to bool safely:
        if verbose_raw is None:
            verbose = False
        else:
            verbose = bool(verbose_raw) 
            chunking = validated.get("chunking", {})

        # Get latest prompt_model_id safely
        query = """
            SELECT COALESCE(MAX(CAST(prompt_model_id AS INT)), 0) + 1 AS model_id
            FROM hive_metastore.fieldforce_navigator_deployment.llm_prompt_model_master_test
        """
        df = execute_query(query)
        model_id_raw = df.iloc[0].get("model_id") if not df.empty else 1

        try:
            model_id = int(model_id_raw)
        except (ValueError, TypeError):
            model_id = 1

        prompt_hash = hashlib.sha256(prompt_template.encode()).hexdigest()
        entry_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Build insert query - use parameterized style if possible, otherwise sanitize carefully
        insert_query = f"""
        INSERT INTO hive_metastore.fieldforce_navigator_deployment.llm_prompt_model_master_test
        VALUES (
            {model_id},
            '{insight_type.replace("'", "''")}',
            '{prompt_template.replace("'", "''")}',
            '{json.dumps(prompt_params).replace("'", "''")}',
            '{prompt_hash}',
            '{llm_model_name.replace("'", "''")}',
            {float(temperature)},
            {int(max_tokens)},
            {str(verbose).lower()},
            timestamp('{entry_time}'),
            '{json.dumps(chunking).replace("'", "''")}',
            '{username.replace("'", "''")}'
        )
        """
        execute_non_query(insert_query)

        return jsonify({"message": f"Prompt inserted with ID {model_id}"}), 200

    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({"error": f"Server error: {str(e)}"}), status.HTTP_500_INTERNAL_SERVER_ERROR
'''
'''@app.route('/update_prompt_template', methods=['POST'])
def update_prompt_template():
    try:
        data = request.get_json(force=True)
        is_valid, validated = validate_prompt_request(data)
        if not is_valid:
            return jsonify({"error": validated}), status.HTTP_400_BAD_REQUEST

        username = request.headers.get("Username", "system")
        insight_type = validated["insight_type"]

        # Get latest prompt_model_id
        query = """SELECT COALESCE(MAX(CAST(prompt_model_id AS INT)), 0) + 1 as model_id
                   FROM hive_metastore.fieldforce_navigator_deployment.llm_prompt_model_master_test"""
        #df = execute_query(query)
        #model_id = df.iloc[0]["model_id"]
        df = execute_query(query)
        model_id_raw = df.iloc[0].get("model_id", 1)

        # Fallback safely if model_id is None or not convertible to int
        try:
            model_id = int(model_id_raw) if model_id_raw is not None else 1
        except (ValueError, TypeError):
            model_id = 1


        prompt_hash = hashlib.sha256(validated["prompt_template"].encode()).hexdigest()
        entry_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        insert_query = f"""
        INSERT INTO hive_metastore.fieldforce_navigator_deployment.llm_prompt_model_master_test
        VALUES (
            '{model_id}',
            '{insight_type}',
            '{validated["prompt_template"]}',
            '{json.dumps(validated["prompt_params"])}',
            '{prompt_hash}',
            '{validated["llm_model_name"]}',
            {validated["temperature"]},
            {validated["max_tokens"]},
            {validated["verbose"]},
            timestamp('{entry_time}'),
            '{json.dumps(validated["chunking"])}',
            '{username}'
        )
        """
        execute_non_query(insert_query)
        return jsonify({"message": f"Prompt inserted with ID {model_id}"}), 200

    except Exception as e:
        logging.error(traceback.format_exc())
        return jsonify({"error": f"Server error: {str(e)}"}), status.HTTP_500_INTERNAL_SERVER_ERROR
'''
#http://localhost:3000/get_prompt_template/hive_metastore/fieldforce_navigator_dev/llm_prompt_model_master?insight_type=agreed-actions
@app.route("/get_prompt_template/<catalog>/<schema>/<table_name>", methods=["GET"])
def get_prompt_template_config(catalog, schema, table_name):
    insight_type = request.args.get("insight_type")
    if not insight_type:
        return jsonify({"error": "Missing required query parameter: insight_type"}), 400
    
    full_table = f"{catalog}.{schema}.{table_name}"
    
    try:
        query = f"""
            SELECT *
            FROM {full_table}
            WHERE insight_type = '{insight_type}'
            ORDER BY entry_time DESC
            LIMIT 5
        """
        df = dc.execute_query(query)
        if df.empty:
            return jsonify({"message": f"No prompt template found for insight_type: {insight_type}"}), 404
        return jsonify(df.to_dict(orient="records")[0]), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
        
#http://localhost:3000/get_data/hive_metastore/fieldforce_navigator_dev/llm_prompt_model_master
@app.route("/get_data/<catalog>/<schema>/<table_name>", methods=["GET"])
def get_data(catalog, schema, table_name):
    try:
        full_table = f"{catalog}.{schema}.{table_name}"
        query = f"""
            SELECT *
            FROM {full_table} 
            LIMIT 5
        """
        df = dc.execute_query(query)
        return jsonify(df.to_dict(orient="records")), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500      


#http://localhost:3000/get_metadata/hive_metastore/fieldforce_navigator_dev/llm_prompt_model_master
@app.route("/get_metadata/<catalog>/<schema>/<table_name>", methods=["GET"])
def get_metadata(catalog, schema, table_name):
    try:
        full_table = f"{catalog}.{schema}.{table_name}"
        query = f"""
            DESCRIBE TABLE {full_table}
        """
        df = dc.execute_query(query)
        return jsonify(df.to_dict(orient="records")), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500




FINAL_INSIGHTS = ['summary-of-summary-insight', 'overall-area-of-interest-insight', 'overall-objections-insight', 'overall-agreed-actions', 'gso-insight', ]
FINAL_INSIGHTS_GUARDRAILS = ['guardrails-summary-of-summary-insight', 'guardrails-overall-area-of-interest-insight', 'guardrails-overall-objections-insight', 'guardrails-overall-agreed-actions' ]

 
def get_valid_rmb_set():
    query = """
        SELECT UPPER(CONCAT(REGION, ' - ', MARKET, ' - ', BRAND)) as RMB
        FROM hive_metastore.fieldforce_navigator_deployment.audio_market_brand_config
    """
    df = dc.execute_query(query)
    print(f"DEBUG: df:\n{df}")

    valid_set = set(df['RMB'].dropna().unique())  # Removes nulls and gets unique values
    for rmb in valid_set:
        print(f"DEBUG: valid_rmb: {rmb}")

    print(f"DEBUG: valid_rmb_set: {valid_set}")
    return valid_set



def validate_prompt_input(data, valid_rmb_set):
    print(f"DEBUG: validate_prompt_input received data: {data}")
    if not isinstance(data, dict):
        print("ERROR: Input data is not a dictionary")
        return False, "Input data must be a JSON object (dict)."

    required_fields = [
        "REGION_MARKET_BRAND", "INSIGHT_TYPE", "PROMPT_ID", "SEQ_NUMBER",
        "POST_PROCESS", "PROCESS_BY_CHUNK", "ACTIVITY_TYPE", "PUBLISH_INSIGHT_NAME"
    ]

    empty_fields = [f for f in required_fields if f not in data or str(data.get(f, "")).strip() == ""]
    if empty_fields:
        error_msg = f"Missing required fields: {', '.join(empty_fields)}"
        print(f"ERROR: {error_msg}")
        return False, error_msg

    rmb_raw = data.get("REGION_MARKET_BRAND")
    print(f"DEBUG: REGION_MARKET_BRAND raw value: {rmb_raw}")

    if not isinstance(rmb_raw, list):
        error_msg = "REGION_MARKET_BRAND must be a list."
        print(f"ERROR: {error_msg}")
        return False, error_msg

    valid_rmb = []
    for item in rmb_raw:
        print(f"DEBUG: Processing RMB item: {item}")
        if not isinstance(item, str):
            error_msg = f"Each REGION_MARKET_BRAND item must be a string. Got: {type(item)}"
            print(f"ERROR: {error_msg}")
            return False, error_msg
        item_upper = item.strip().upper()
        print(f"item_upper: {item_upper}")
        for rmb in valid_rmb_set:
            print(f"RMB: {rmb}")

        if item_upper not in valid_rmb_set:
            error_msg = f"Invalid REGION_MARKET_BRAND: {item}"
            print(f"ERROR: {error_msg}")
            return False, error_msg

        parts = item_upper.split(" - ")
        if len(parts) != 3:
            error_msg = f"REGION_MARKET_BRAND must be in 'REGION - MARKET - BRAND' format. Got: {item}"
            print(f"ERROR: {error_msg}")
            return False, error_msg

        region, market, brand = parts
        valid_rmb.append((region, market, brand))

    if not valid_rmb:
        error_msg = "REGION_MARKET_BRAND must contain at least one valid entry."
        print(f"ERROR: {error_msg}")
        return False, error_msg

    insight_type = data["INSIGHT_TYPE"].strip().lower()
    publish_name = data["PUBLISH_INSIGHT_NAME"].strip()
    region = valid_rmb[0][0]
    print(f"DEBUG: insight_type: {insight_type}, publish_name: {publish_name}, region: {region}")

    if insight_type in FINAL_INSIGHTS and publish_name == '-' and region != 'FRA':
        error_msg = "Please provide a publish_insight_name"
        print(f"ERROR: {error_msg}")
        return False, error_msg
    elif insight_type in FINAL_INSIGHTS_GUARDRAILS and publish_name == '-':
        error_msg = "Please provide a publish_insight_name"
        print(f"ERROR: {error_msg}")
        return False, error_msg
    elif insight_type not in FINAL_INSIGHTS + FINAL_INSIGHTS_GUARDRAILS and publish_name != '-':
        error_msg = "Unexpected publish_insight_name for given insight_type"
        print(f"ERROR: {error_msg}")
        return False, error_msg

    print("validate_prompt_input returning:", valid_rmb)
    return True, valid_rmb


def validate_model_id(insight_type, prompt_id):
    query = f"""
        SELECT * FROM hive_metastore.fieldforce_navigator_deployment.llm_prompt_model_master
        WHERE UPPER(TRIM(INSIGHT_TYPE)) = UPPER(TRIM('{insight_type}'))
        AND UPPER(TRIM(PROMPT_MODEL_ID)) = UPPER(TRIM('{prompt_id}'))
    """

    df = dc.execute_query(query)
    print(f"DEBUG: df =\n{df}")

    if df is None or df.empty:
        print("Prompt ID and Insight_type don't match")
        return False

    return True


def validate_seq_uniqueness(brand, market, region, insight_type, seq, activity_type):
    print(f"DEBUG: validate_seq_uniqueness called with brand={brand}, market={market}, region={region}, insight_type={insight_type}, seq={seq}, activity_type={activity_type}")
    query = f"""
        SELECT * FROM hive_metastore.fieldforce_navigator_deployment.llm_insight_seq
        WHERE BRAND = '{brand}' AND MARKET = '{market}' AND REGION = '{region}'
        AND SEQ = {seq} AND upper(IS_ACTIVE) = 'TRUE'
        AND upper(ACTIVITY_TYPE) = '{activity_type.upper()}'
        AND UPPER(INSIGHT_TYPE) not in ('{insight_type}')
    """
    df = dc.execute_query(query)
    result = len(df) == 0
    print(f"DEBUG: validate_seq_uniqueness result: {result}")
    return result

def disable_existing_prompt(region, market, brand, insight_type, activity_type):
    print(f"DEBUG: disable_existing_prompt called with region={region}, market={market}, brand={brand}, insight_type={insight_type}, activity_type={activity_type}")
    query = f"""
        UPDATE hive_metastore.fieldforce_navigator_deployment.llm_insight_seq
        SET IS_ACTIVE = 'FALSE'
        WHERE REGION = '{region}' AND MARKET = '{market}' AND BRAND = '{brand}'
        AND INSIGHT_TYPE = ('{insight_type}') AND ACTIVITY_TYPE = '{activity_type}'
        AND IS_ACTIVE = TRUE
    """
    dc.execute_non_query(query)


def get_next_seq(region, market, brand, activity_type):
    print(f"DEBUG: get_next_seq called with region={region}, market={market}, brand={brand}, activity_type={activity_type}")
    
    query = f"""
        SELECT CAST(COALESCE(MAX(INT(SEQ)), 0) + 1 AS STRING) AS next_seq
        FROM hive_metastore.fieldforce_navigator_deployment.llm_insight_seq
        WHERE REGION = '{region}' AND MARKET = '{market}' AND BRAND = '{brand}'
        AND UPPER(ACTIVITY_TYPE) = '{activity_type.upper()}'
    """
    
    df = dc.execute_query(query)
    next_seq = "1"  # default as string

    if df is not None and not df.empty and 'next_seq' in df.columns:
        value = df.iloc[0]['next_seq']
        if value is not None:
            next_seq = value

    print(f"DEBUG: next_seq computed: {next_seq}")
    return next_seq


def insert_prompt_row(region, market, brand, insight_type, seq, prompt_id, post_process, activity_type, process_by_chunk, publish_insight_name, username):
    print(f"DEBUG: insert_prompt_row called with region={region}, market={market}, brand={brand}, insight_type={insight_type}, seq={seq}, prompt_id={prompt_id}, post_process={post_process}, activity_type={activity_type}, process_by_chunk={process_by_chunk}, publish_insight_name={publish_insight_name}, username={username}")
    # Convert booleans to SQL-compatible literals
    post_process_sql = 'TRUE' if post_process else 'FALSE'
    process_by_chunk_sql = 'TRUE' if process_by_chunk else 'FALSE'
    query = f"""
        INSERT INTO hive_metastore.fieldforce_navigator_deployment.llm_insight_seq
        (INSIGHT_TYPE, BRAND, MARKET, REGION, SEQ, IS_ACTIVE, PROMPT_ID, ENTRY_TIME,
        ADDED_BY, POST_PROCESS, ACTIVITY_TYPE, PROCESS_BY_CHUNK, PUBLISH_INSIGHT_NAME)
        VALUES
        ('{insight_type}', '{brand}', '{market}', '{region}', {seq}, 'TRUE', '{prompt_id}',
         CURRENT_TIMESTAMP(), '{username}', {post_process_sql}, '{activity_type}', {process_by_chunk_sql}, '{publish_insight_name}')
    """
    print(f"DEBUG: Insert Query: {query}")
    dc.execute_non_query(query)

@app.route('/Add_Prompt_seq', methods=['POST'])
def prompt_seq_route():
    try:
        data = request.get_json(force=True)
        print(f"DEBUG: Received request data: {data}")

        username = request.headers.get("Username")
        print(f"DEBUG: Username from headers: {username}")
        if not username:
            return jsonify({"error": "Missing Username in headers."}), status.HTTP_400_BAD_REQUEST

        valid_rmb_set = get_valid_rmb_set()
        is_valid, result = validate_prompt_input(data, valid_rmb_set)
        if not is_valid:
            return jsonify({"error": result}), status.HTTP_400_BAD_REQUEST

        # Safety check before unpacking
        if not isinstance(result, list) or not all(isinstance(x, (list, tuple)) and len(x) == 3 for x in result):
            error_msg = "Internal Error: Invalid format returned by validation."
            print(f"ERROR: {error_msg}, result={result}")
            return jsonify({"error": error_msg}), status.HTTP_500_INTERNAL_SERVER_ERROR

        insight_type = data.get('INSIGHT_TYPE', '').strip()
        prompt_id = data.get('PROMPT_ID', '').strip()
        seq_number = data.get('SEQ_NUMBER')
        post_process = str(data.get('POST_PROCESS', '')).strip().lower() == 'true'
        process_by_chunk = str(data.get('PROCESS_BY_CHUNK', '')).strip().lower() == 'true'
        activity_type = data.get('ACTIVITY_TYPE', '').strip()
        publish_name = data.get('PUBLISH_INSIGHT_NAME', '').strip()

        print(f"DEBUG: Parsed fields: insight_type={insight_type}, prompt_id={prompt_id}, seq_number={seq_number}, post_process={post_process}, process_by_chunk={process_by_chunk}, activity_type={activity_type}, publish_name={publish_name}")

        # Check if model ID is valid
        if not validate_model_id(insight_type, prompt_id):
            return jsonify({"error": f"Invalid prompt_id '{prompt_id}' for insight_type '{insight_type}'"}), status.HTTP_400_BAD_REQUEST
       
            
        for region, market, brand in result:
            print(f"DEBUG: Processing RMB triple: region={region}, market={market}, brand={brand}")

            # If seq_number is 'next_seq', calculate next sequence number
            if str(seq_number).strip().lower() == "next_seq":
                seq = get_next_seq(region, market, brand, activity_type)
            else:
                try:
                    seq = int(seq_number)
                except Exception as e:
                    error_msg = f"Invalid SEQ_NUMBER: {seq_number}. Must be integer or 'next_seq'."
                    print(f"ERROR: {error_msg}")
                    return jsonify({"error": error_msg}), status.HTTP_400_BAD_REQUEST

            # Check sequence uniqueness
            if not validate_seq_uniqueness(brand, market, region, insight_type, seq, activity_type):
                error_msg = f"Sequence number {seq} already exists for other insight types in {region} - {market} - {brand}."
                print(f"ERROR: {error_msg}")
                return jsonify({"error": error_msg}), status.HTTP_400_BAD_REQUEST

            # Disable any existing active prompt for this RMB and insight_type
            disable_existing_prompt(region, market, brand, insight_type, activity_type)

            # Insert new prompt row
            insert_prompt_row(region, market, brand, insight_type, seq, prompt_id,
                              post_process, activity_type, process_by_chunk, publish_name, username)

        return jsonify({"message": "Prompt sequence(s) successfully processed."}), status.HTTP_200_OK

    except Exception as e:
        print("ERROR: Exception in /prompt_seq route:", e)
        traceback.print_exc()
        return jsonify({"error": "Internal server error."}), status.HTTP_500_INTERNAL_SERVER_ERROR




@app.route('/deploy_default_hcp_config', methods=['POST'])
def update_default_hcp_config():
    try:
        data = request.get_json(force=True, silent=True) or {}
        username = request.headers.get("Username")

        if not username:
            return jsonify({"error": "Missing 'Username' in headers."}), status.HTTP_400_BAD_REQUEST

        # Optional overrides
        override_region = data.get("REGION")
        override_market = data.get("MARKET")
        override_brand = data.get("BRAND")

        # Validate and normalize RMBs
        valid_rmbs = get_valid_rmb_set()
        if override_region and override_market and override_brand:
            override_rmb = (
                override_region.strip().lower(),
                override_market.strip().lower(),
                override_brand.strip().lower()
            )
            if override_rmb not in valid_rmbs:
                return jsonify({"error": f"Invalid BRAND-MARKET-REGION combination: {override_rmb}"}), status.HTTP_400_BAD_REQUEST
            rmbs = [tuple(x.upper() for x in override_rmb)]
        else:
            rmbs = [tuple(x.upper() for x in rmb) for rmb in valid_rmbs]

        # Default hardcoded config from CSV
        default_config = [
            {"INSIGHT_TYPE": "medical-word-reassignment", "SEQ": 1, "PROMPT_ID": 176, "POST_PROCESS": True,  "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "speaker-assignment",        "SEQ": 2, "PROMPT_ID": 53,  "POST_PROCESS": True,  "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "summary-insight",           "SEQ": 3, "PROMPT_ID": 159, "POST_PROCESS": False, "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "summary-of-summary-insight","SEQ": 4, "PROMPT_ID": 160, "POST_PROCESS": True,  "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "SUMMARY_OF_SUMMARY_INSIGHT"},
            {"INSIGHT_TYPE": "area-of-interest-insight",  "SEQ": 5, "PROMPT_ID": 161, "POST_PROCESS": False, "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "overall-area-of-interest-insight","SEQ": 6,"PROMPT_ID":162,"POST_PROCESS": True,"ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "OVERALL_AREA_OF_INTEREST_INSIGHT"},
            {"INSIGHT_TYPE": "objections-insight",        "SEQ": 7, "PROMPT_ID": 163, "POST_PROCESS": False, "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "overall-objections-insight","SEQ": 8, "PROMPT_ID": 164, "POST_PROCESS": True,  "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "OVERALL_OBJECTIONS"},
            {"INSIGHT_TYPE": "agreed-actions",            "SEQ": 9, "PROMPT_ID": 165, "POST_PROCESS": False, "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "overall-agreed-actions",    "SEQ": 10,"PROMPT_ID": 166, "POST_PROCESS": True,  "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "OVERALL_AGREED_ACTIONS"}
        ]

        for region, market, brand in rmbs:
            for cfg in default_config:
                insight_type = cfg.get("INSIGHT_TYPE")
                prompt_id = str(cfg.get("PROMPT_ID")).strip()
                post_process = cfg.get("POST_PROCESS")
                activity_type = cfg.get("ACTIVITY_TYPE")
                process_by_chunk = cfg.get("PROCESS_BY_CHUNK")
                publish_name = cfg.get("PUBLISH_INSIGHT_NAME")
                seq = cfg.get("SEQ")

                # ---------- Validations --------
                # Validate prompt exists
                if not validate_prompt_input(prompt_id):
                    return jsonify({"error": f"PROMPT_ID {prompt_id} not found in prompt_templates."}), status.HTTP_400_BAD_REQUEST

                # Validate prompt is mapped to insight
                if not validate_model_id(insight_type, prompt_id):
                    return jsonify({"error": f"PROMPT_ID {prompt_id} not valid for INSIGHT_TYPE '{insight_type}'"}), status.HTTP_400_BAD_REQUEST

                # Validate SEQ uniqueness
                if not validate_seq_uniqueness(region, market, brand, insight_type, seq):
                    return jsonify({"error": f"SEQ {seq} already used for {insight_type} in {region}-{market}-{brand}"}), status.HTTP_400_BAD_REQUEST

                '''if str(seq).lower() == "next_seq":
                    seq = get_next_seq(region, market, brand, insight_type)'''

                # ---------- Data Write ----------
                disable_existing_prompt(region, market, brand, insight_type, activity_type)
                insert_prompt_row(region, market, brand, insight_type, seq, prompt_id,
                                  post_process, activity_type, process_by_chunk, publish_name, username)

        return jsonify({"message": "Default configuration updated successfully."}), status.HTTP_200_OK

    except Exception as e:
        print("ERROR:", e)
        traceback.print_exc()
        return jsonify({"error": "Internal server error."}), status.HTTP_500_INTERNAL_SERVER_ERROR



@app.route('/deploy_default_self_config', methods=['POST'])
def update_default_self_config():
    try:
        data = request.get_json(force=True, silent=True) or {}
        username = request.headers.get("Username")

        if not username:
            return jsonify({"error": "Missing 'Username' in headers."}), status.HTTP_400_BAD_REQUEST

        # Optional overrides
        override_region = data.get("REGION")
        override_market = data.get("MARKET")
        override_brand = data.get("BRAND")

        # Validate and normalize RMBs
        valid_rmbs = get_valid_rmb_set()
        if override_region and override_market and override_brand:
            override_rmb = (
                override_region.strip().lower(),
                override_market.strip().lower(),
                override_brand.strip().lower()
            )
            if override_rmb not in valid_rmbs:
                return jsonify({"error": f"Invalid BRAND-MARKET-REGION combination: {override_rmb}"}), status.HTTP_400_BAD_REQUEST
            rmbs = [tuple(x.upper() for x in override_rmb)]
        else:
            rmbs = [tuple(x.upper() for x in rmb) for rmb in valid_rmbs]

        # Default config from CSV provided
        default_config = [
            {"INSIGHT_TYPE": "medical-word-reassignment", "SEQ": 1,  "PROMPT_ID": 176, "POST_PROCESS": True,  "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "speaker-assignment",        "SEQ": 2,  "PROMPT_ID": 68,  "POST_PROCESS": True,  "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "area-of-interest-insight",  "SEQ": 5,  "PROMPT_ID": 169, "POST_PROCESS": False, "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "overall-area-of-interest-insight", "SEQ": 6, "PROMPT_ID": 170, "POST_PROCESS": True,  "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "OVERALL_AREA_OF_INTEREST_INSIGHT"},
            {"INSIGHT_TYPE": "objections-insight",        "SEQ": 7,  "PROMPT_ID": 171, "POST_PROCESS": False, "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "overall-objections-insight","SEQ": 8,  "PROMPT_ID": 172, "POST_PROCESS": True,  "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "OVERALL_OBJECTIONS"},
            {"INSIGHT_TYPE": "agreed-actions",            "SEQ": 9,  "PROMPT_ID": 173, "POST_PROCESS": False, "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "overall-agreed-actions",    "SEQ": 10, "PROMPT_ID": 174, "POST_PROCESS": True,  "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "OVERALL_AGREED_ACTIONS"},
            {"INSIGHT_TYPE": "summary-insight",           "SEQ": 13, "PROMPT_ID": 167, "POST_PROCESS": False, "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "summary-of-summary-insight","SEQ": 14, "PROMPT_ID": 168, "POST_PROCESS": True,  "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "SUMMARY_OF_SUMMARY_INSIGHT"}
        ]

        for region, market, brand in rmbs:
            for cfg in default_config:
                insight_type = cfg.get("INSIGHT_TYPE")
                prompt_id = str(cfg.get("PROMPT_ID")).strip()
                post_process = cfg.get("POST_PROCESS")
                activity_type = cfg.get("ACTIVITY_TYPE")
                process_by_chunk = cfg.get("PROCESS_BY_CHUNK")
                publish_name = cfg.get("PUBLISH_INSIGHT_NAME")
                seq = cfg.get("SEQ")

                # ---------- Validations ----------
                # Validate prompt exists
                if not validate_prompt_input(prompt_id):
                    return jsonify({"error": f"PROMPT_ID {prompt_id} not found in prompt_templates."}), status.HTTP_400_BAD_REQUEST

                # Validate prompt is mapped to insight
                if not validate_model_id(insight_type, prompt_id):
                    return jsonify({"error": f"PROMPT_ID {prompt_id} not valid for INSIGHT_TYPE '{insight_type}'"}), status.HTTP_400_BAD_REQUEST

                # Validate SEQ uniqueness
                if not validate_seq_uniqueness(region, market, brand, insight_type, seq):
                    return jsonify({"error": f"SEQ {seq} already used for {insight_type} in {region}-{market}-{brand}"}), status.HTTP_400_BAD_REQUEST

                # ---------- Data Write ----------
                disable_existing_prompt(region, market, brand, insight_type, activity_type)
                insert_prompt_row(region, market, brand, insight_type, seq, prompt_id,
                                  post_process, activity_type, process_by_chunk, publish_name, username)

        return jsonify({"message": "Default configuration updated successfully."}), status.HTTP_200_OK

    except Exception as e:
        print("ERROR:", e)
        traceback.print_exc()
        return jsonify({"error": "Internal server error."}), status.HTTP_500_INTERNAL_SERVER_ERROR



@app.route('/deploy_default_config', methods=['POST'])
def deploy_default_configs():
    try:
        data = request.get_json(force=True, silent=True) or {}
        username = request.headers.get("Username")

        if not username:
            return jsonify({"error": "Missing 'Username' in headers."}), status.HTTP_400_BAD_REQUEST

        # Required: BRAND, MARKET, REGION
        region = data.get("REGION")
        market = data.get("MARKET")
        brand = data.get("BRAND")

        if not all([region, market, brand]):
            return jsonify({"error": "Missing REGION, MARKET, or BRAND in request body."}), status.HTTP_400_BAD_REQUEST

        # Normalize and validate
        input_rmb = (region.strip(), market.strip(), brand.strip())
        valid_rmbs = get_valid_rmb_set()
        if input_rmb not in valid_rmbs:
            return jsonify({"error": f"Invalid BRAND-MARKET-REGION combination: {input_rmb}"}), status.HTTP_400_BAD_REQUEST

        rmb = tuple(x.upper() for x in input_rmb)
        region, market, brand = rmb

        # Combined HCP + Self configs (with activity_type inside configs only)
        combined_config = [
            # HCP configs
            {"INSIGHT_TYPE": "medical-word-reassignment", "SEQ": 1, "PROMPT_ID": 176, "POST_PROCESS": True, "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "speaker-assignment", "SEQ": 2, "PROMPT_ID": 53, "POST_PROCESS": True, "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "summary-insight", "SEQ": 3, "PROMPT_ID": 159, "POST_PROCESS": False, "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "summary-of-summary-insight", "SEQ": 4, "PROMPT_ID": 160, "POST_PROCESS": True, "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "SUMMARY_OF_SUMMARY_INSIGHT"},
            {"INSIGHT_TYPE": "area-of-interest-insight", "SEQ": 5, "PROMPT_ID": 161, "POST_PROCESS": False, "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "overall-area-of-interest-insight", "SEQ": 6, "PROMPT_ID": 162, "POST_PROCESS": True, "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "OVERALL_AREA_OF_INTEREST_INSIGHT"},
            {"INSIGHT_TYPE": "objections-insight", "SEQ": 7, "PROMPT_ID": 163, "POST_PROCESS": False, "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "overall-objections-insight", "SEQ": 8, "PROMPT_ID": 164, "POST_PROCESS": True, "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "OVERALL_OBJECTIONS"},
            {"INSIGHT_TYPE": "agreed-actions", "SEQ": 9, "PROMPT_ID": 165, "POST_PROCESS": False, "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "overall-agreed-actions", "SEQ": 10, "PROMPT_ID": 166, "POST_PROCESS": True, "ACTIVITY_TYPE": "HCP", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "OVERALL_AGREED_ACTIONS"},
            # Self configs
            {"INSIGHT_TYPE": "medical-word-reassignment", "SEQ": 1, "PROMPT_ID": 176, "POST_PROCESS": True, "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "speaker-assignment", "SEQ": 2, "PROMPT_ID": 68, "POST_PROCESS": True, "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "area-of-interest-insight", "SEQ": 5, "PROMPT_ID": 169, "POST_PROCESS": False, "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "overall-area-of-interest-insight", "SEQ": 6, "PROMPT_ID": 170, "POST_PROCESS": True, "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "OVERALL_AREA_OF_INTEREST_INSIGHT"},
            {"INSIGHT_TYPE": "objections-insight", "SEQ": 7, "PROMPT_ID": 171, "POST_PROCESS": False, "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "overall-objections-insight", "SEQ": 8, "PROMPT_ID": 172, "POST_PROCESS": True, "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "OVERALL_OBJECTIONS"},
            {"INSIGHT_TYPE": "agreed-actions", "SEQ": 9, "PROMPT_ID": 173, "POST_PROCESS": False, "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "overall-agreed-actions", "SEQ": 10, "PROMPT_ID": 174, "POST_PROCESS": True, "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "OVERALL_AGREED_ACTIONS"},
            {"INSIGHT_TYPE": "summary-insight", "SEQ": 13, "PROMPT_ID": 167, "POST_PROCESS": False, "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "-"},
            {"INSIGHT_TYPE": "summary-of-summary-insight", "SEQ": 14, "PROMPT_ID": 168, "POST_PROCESS": True, "ACTIVITY_TYPE": "Self", "PROCESS_BY_CHUNK": False, "PUBLISH_INSIGHT_NAME": "SUMMARY_OF_SUMMARY_INSIGHT"}
        ]

        for cfg in combined_config:
            insight_type = cfg.get("INSIGHT_TYPE")
            prompt_id = str(cfg.get("PROMPT_ID")).strip()
            post_process = cfg.get("POST_PROCESS")
            activity_type = cfg.get("ACTIVITY_TYPE")
            process_by_chunk = cfg.get("PROCESS_BY_CHUNK")
            publish_name = cfg.get("PUBLISH_INSIGHT_NAME")
            seq = cfg.get("SEQ")

            # --------- Validations ----------
            if not validate_prompt_input(prompt_id):
                return jsonify({"error": f"PROMPT_ID {prompt_id} not found in prompt_templates."}), status.HTTP_400_BAD_REQUEST

            if not validate_model_id(insight_type, prompt_id):
                return jsonify({"error": f"PROMPT_ID {prompt_id} not valid for INSIGHT_TYPE '{insight_type}'"}), status.HTTP_400_BAD_REQUEST

            if not validate_seq_uniqueness(region, market, brand, insight_type, seq):
                return jsonify({"error": f"SEQ {seq} already used for {insight_type} in {region}-{market}-{brand}"}), status.HTTP_400_BAD_REQUEST

            # --------- Write ---------
            disable_existing_prompt(region, market, brand, insight_type, activity_type)
            insert_prompt_row(region, market, brand, insight_type, seq, prompt_id,
                              post_process, activity_type, process_by_chunk, publish_name, username)

        return jsonify({"message": "Default HCP and Self configuration deployed successfully."}), status.HTTP_200_OK

    except Exception as e:
        print("ERROR:", e)
        traceback.print_exc()
        return jsonify({"error": "Internal server error."}), status.HTTP_500_INTERNAL_SERVER_ERROR




def validate_required_fields(data, required_fields):
    missing = [f for f in required_fields if f not in data or data[f] is None or str(data[f]).strip() == ""]
    return missing

def validate_rmb_unique(region, market, brand):
    """
    Check if a row already exists in audio_market_brand_config with the same REGION, MARKET, BRAND
    Returns True if unique (safe to insert), False if already exists.
    """
    try:
        query = f"""
            SELECT *
            FROM hive_metastore.fieldforce_navigator_deployment.audio_market_brand_config
            WHERE REGION = '{region}'
              AND MARKET = '{market}'
              AND BRAND = '{brand}'
        """
    
        df = dc.execute_query(query)
        result = len(df) == 0
        return result
        
    except Exception as e:
        print("Error validating RMB uniqueness:", e)
        raise


def insert_audio_market_brand_config(region, market, brand,
                                     transcript_lang, pii_lang, translation_lang,
                                     llm_default_prompt_flag, language,
                                     username,
                                     no_of_archival_days=10,
                                     bypass_llm_translation_flag=True,
                                     no_of_past_summaries=4):
    """
    Inserts a new record into audio_market_brand_config.
    """
    #entry_time = datetime.utcnow().isoformat() + "Z"
    entry_time = datetime.datetime.utcnow().isoformat() + "Z"
    insert_query = f"""
        INSERT INTO hive_metastore.fieldforce_navigator_deployment.audio_market_brand_config (
            REGION, MARKET, BRAND,
            TRANSCRIPT_LANGUAGE, PII_REMOVAL_LANGUAGE, TRANSLATION_LANGUAGE,
            NO_OF_ARCHIVAL_DAYS, BYPASS_LLM_TRANSLATION_FLAG, ADDED_BY, ENTRY_TIME,
            LLM_DEFAULT_PROMPT_FLAG, LANGUAGE, NO_OF_PAST_SUMMARIES
        )
        VALUES (
            '{region}', '{market}', '{brand}',
            '{transcript_lang}', '{pii_lang}', '{translation_lang}',
            {no_of_archival_days}, {str(bypass_llm_translation_flag).lower()}, '{username}', '{entry_time}',
            {str(llm_default_prompt_flag).lower()}, '{language}', {no_of_past_summaries}
        )
    """
    dc.execute_non_query(insert_query) 



@app.route('/new_market_onboard_old', methods=['POST'])
def add_audio_market_brand_config():
    try:
        data = request.get_json(force=True, silent=True) or {}
        username = request.headers.get("Username")

        if not username:
            return jsonify({"error": "Missing 'Username' in headers."}), status.HTTP_400_BAD_REQUEST

        required_fields = [
            "REGION",
            "MARKET",
            "BRAND",
            "TRANSCRIPT_LANGUAGE",
            "PII_REMOVAL_LANGUAGE",
            "TRANSLATION_LANGUAGE",
            "LLM_DEFAULT_PROMPT_FLAG",
            "LANGUAGE"
        ]
        missing_fields = validate_required_fields(data, required_fields)
        if missing_fields:
            return jsonify({"error": f"Missing required fields: {', '.join(missing_fields)}"}), status.HTTP_400_BAD_REQUEST

        # Extract fields
        region = str(data["REGION"]).strip()
        market = str(data["MARKET"]).strip()
        brand = str(data["BRAND"]).strip()
        transcript_lang = str(data["TRANSCRIPT_LANGUAGE"]).strip()
        pii_lang = str(data["PII_REMOVAL_LANGUAGE"]).strip()
        translation_lang = str(data["TRANSLATION_LANGUAGE"]).strip()
        llm_default_prompt_flag = str(data["LLM_DEFAULT_PROMPT_FLAG"]).strip().lower() in ("true", "1", "yes")
        language = str(data["LANGUAGE"]).strip()

        # Validate uniqueness
        if not validate_rmb_unique(region, market, brand):
            return jsonify({"error": f"Configuration already exists for {region}-{market}-{brand}."}), status.HTTP_400_BAD_REQUEST

        # Call insert helper
        insert_audio_market_brand_config(
            region=region,
            market=market,
            brand=brand,
            transcript_lang=transcript_lang,
            pii_lang=pii_lang,
            translation_lang=translation_lang,
            llm_default_prompt_flag=llm_default_prompt_flag,
            language=language,
            username=username
        )

        return jsonify({"message": f"Configuration added for {region}-{market}-{brand}"}), status.HTTP_201_CREATED

    except Exception as e:
        print("ERROR in add_audio_market_brand_config:", e)
        traceback.print_exc()
        return jsonify({"error": "Internal server error."}), status.HTTP_500_INTERNAL_SERVER_ERROR



@app.route("/get_default_prompts/", methods=["GET"])
def get_default_prompts():
    try:

        query = f"""
            SELECT *
            FROM hive_metastore.fieldforce_navigator_deployment.llm_insight_seq_default
        """
        df = dc.execute_query(query)
        return jsonify(df.to_dict(orient="records")), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500      


@app.route("/get_default_prompts_metadata/", methods=["GET"])
def get_default_prompts_metadata():
    try:
        
        query = f"""
            DESCRIBE TABLE hive_metastore.fieldforce_navigator_deployment.llm_insight_seq_default
        """
        df = dc.execute_query(query)
        return jsonify(df.to_dict(orient="records")), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500




def validate_seq_uniqueness_default(insight_type, seq, activity_type, prompt_id):
    """
    Check if given seq already exists for another (insight_type,prompt_id) combination
    with same activity_type.
    """
    print(f"DEBUG: validate_seq_uniqueness_default called with insight_type={insight_type}, prompt_id={prompt_id}, seq={seq}, activity_type={activity_type}")
    query = f"""
      SELECT * FROM hive_metastore.fieldforce_navigator_deployment.llm_insight_seq_default
        WHERE SEQ = {seq} AND upper(IS_ACTIVE) = 'TRUE'
        AND upper(ACTIVITY_TYPE) = '{activity_type.upper()}'
        AND UPPER(INSIGHT_TYPE) in ('{insight_type}')
        AND PROMPT_ID = '{prompt_id}'
    """
    df = dc.execute_query(query)
    result = df.empty  # true if no conflicting rows
    print(f"DEBUG: validate_seq_uniqueness_default result: {result}")
    return result


def disable_existing_prompt_default(insight_type, activity_type, seq, prompt_id):
    """
    Disable any existing active prompt row for given insight_type, prompt_id & activity_type.
    """
    print(f"DEBUG: disable_existing_prompt_default called with insight_type={insight_type}, prompt_id={prompt_id}, activity_type={activity_type}, seq={seq}")
    query = f"""
        UPDATE hive_metastore.fieldforce_navigator_deployment.llm_insight_seq_default
        SET IS_ACTIVE = 'FALSE'
        WHERE INSIGHT_TYPE = '{insight_type}'
          AND PROMPT_ID = '{prompt_id}'
          AND ACTIVITY_TYPE = '{activity_type}'
          AND SEQ = '{seq}'
          AND IS_ACTIVE = TRUE
    """
    dc.execute_non_query(query)


def get_next_seq_default(insight_type, activity_type, prompt_id):
    """
    Get next available sequence number for given insight_type, prompt_id and activity_type.
    """
    print(f"DEBUG: get_next_seq_default called with insight_type={insight_type}, prompt_id={prompt_id}, activity_type={activity_type}")
    query = f"""
        SELECT CAST(COALESCE(MAX(CAST(SEQ AS INT)), 0) + 1 AS STRING) AS next_seq
        FROM hive_metastore.fieldforce_navigator_deployment.llm_insight_seq_default
        WHERE UPPER(ACTIVITY_TYPE) = '{activity_type.upper()}'
          AND UPPER(INSIGHT_TYPE) = '{insight_type.upper()}'
          AND UPPER(PROMPT_ID) = '{prompt_id.upper()}'
    """
    df = dc.execute_query(query)
    next_seq = "1"
    if df is not None and not df.empty and 'next_seq' in df.columns:
        value = df.iloc[0]['next_seq']
        if value is not None:
            next_seq = value
    print(f"DEBUG: next_seq computed: {next_seq}")
    return next_seq


def insert_prompt_row_default(insight_type, seq, prompt_id, post_process, activity_type,
                              process_by_chunk, publish_insight_name, username):
    """
    Insert a new prompt row into llm_insight_seq_default table.
    """
    print(f"DEBUG: insert_prompt_row_default called with insight_type={insight_type}, prompt_id={prompt_id}, seq={seq}, post_process={post_process}, activity_type={activity_type}, process_by_chunk={process_by_chunk}, publish_insight_name={publish_insight_name}, username={username}")

    post_process_sql = 'TRUE' if post_process else 'FALSE'
    process_by_chunk_sql = 'TRUE' if process_by_chunk else 'FALSE'

    query = f"""
        INSERT INTO hive_metastore.fieldforce_navigator_deployment.llm_insight_seq_default
        (INSIGHT_TYPE, SEQ, IS_ACTIVE, PROMPT_ID, ENTRY_TIME,
         ADDED_BY, POST_PROCESS, ACTIVITY_TYPE, PROCESS_BY_CHUNK, PUBLISH_INSIGHT_NAME)
        VALUES
        ('{insight_type}', {seq}, 'TRUE', '{prompt_id}', CURRENT_TIMESTAMP(),
         '{username}', {post_process_sql}, '{activity_type}', {process_by_chunk_sql}, '{publish_insight_name}')
    """
    print(f"DEBUG: Insert Query: {query}")
    dc.execute_non_query(query)


@app.route('/Update_default_prompt', methods=['POST'])
def prompt_seq_route_default():
    try:
        data = request.get_json(force=True)
        print(f"DEBUG: Received request data: {data}")

        username = request.headers.get("Username")
        if not username:
            return jsonify({"error": "Missing Username in headers."}), status.HTTP_400_BAD_REQUEST

        insight_type = data.get('INSIGHT_TYPE', '').strip()
        prompt_id = data.get('PROMPT_ID', '').strip()
        seq_number = data.get('SEQ_NUMBER')
        post_process = str(data.get('POST_PROCESS', '')).strip().lower() == 'true'
        process_by_chunk = str(data.get('PROCESS_BY_CHUNK', '')).strip().lower() == 'true'
        activity_type = data.get('ACTIVITY_TYPE', '').strip()
        publish_name = data.get('PUBLISH_INSIGHT_NAME', '').strip()

        print(f"DEBUG: Parsed fields: insight_type={insight_type}, prompt_id={prompt_id}, seq_number={seq_number}, post_process={post_process}, process_by_chunk={process_by_chunk}, activity_type={activity_type}, publish_name={publish_name}")

        # Validate model ID
        if not validate_model_id(insight_type, prompt_id):
            return jsonify({"error": f"Invalid prompt_id '{prompt_id}' for insight_type '{insight_type}'"}), status.HTTP_400_BAD_REQUEST

        # Resolve sequence
        if str(seq_number).strip().lower() == "next_seq":
            seq = get_next_seq_default(insight_type, activity_type, prompt_id)
        else:
            try:
                seq = int(seq_number)
            except Exception:
                return jsonify({"error": f"Invalid SEQ_NUMBER: {seq_number}. Must be integer or 'next_seq'."}), status.HTTP_400_BAD_REQUEST

        # Validate sequence uniqueness
        if not validate_seq_uniqueness_default(insight_type, seq, activity_type, prompt_id):
            return jsonify({"error": f"Sequence number {seq} already exists for another insight_type {insight_type}, prompt_id {prompt_id} with this activity type."}), status.HTTP_400_BAD_REQUEST

        # Disable existing active prompt
        disable_existing_prompt_default(insight_type, activity_type, seq, prompt_id)

        # Insert new row
        insert_prompt_row_default(insight_type, seq, prompt_id, post_process, activity_type, process_by_chunk, publish_name, username)

        return jsonify({"message": "Prompt sequence successfully processed."}), status.HTTP_200_OK

    except Exception as e:
        print("ERROR: Exception in /prompt_seq_default:", e)
        traceback.print_exc()
        return jsonify({"error": "Internal server error."}), status.HTTP_500_INTERNAL_SERVER_ERROR


#New llm_prompt into Default table 
@app.route('/add_new_llm_prompt_default', methods=['POST'])
def add_variable_config_route():
    try:
        data = request.get_json(force=True)
        print(f"DEBUG: Received data: {data}")

        username = request.headers.get("Username")
        if not username:
            return jsonify({"error": "Missing Username in headers."}), status.HTTP_400_BAD_REQUEST

        # Prompt Model Validations
        is_valid, result = validate_prompt_request(data)
        if not is_valid:
            return jsonify({"error": result}), status.HTTP_400_BAD_REQUEST

        validated_data = result
        model_id = insert_prompt_model(validated_data, username)

        # Insert variables from JSON input
        variables_list = data.get("variables", [])
        prompt_id = str(model_id)

        for variable in variables_list:
            required_keys = ["VARIABLE", "INPUT_TABLE_NAME", "INPUT_COL_NAME", "INPUT_FILTERS", "TO_BE_CHUNKED", "INPUT_PROCESSING", "IS_ACTIVE"]
            for key in required_keys:
                if key not in variable:
                    return jsonify({"error": f"Missing key '{key}' in variable config."}), status.HTTP_400_BAD_REQUEST

            insert_query = f"""
                INSERT INTO hive_metastore.fieldforce_navigator_deployment.llm_prompt_input_data_config_default
                (PROMPT_ID, VARIABLE, INPUT_TABLE_NAME, INPUT_COL_NAME, INPUT_FILTERS, TO_BE_CHUNKED, INPUT_PROCESSING, IS_ACTIVE, ENTRY_TIME, ADDED_BY)
                VALUES (
                    '{prompt_id}',
                    '{variable["VARIABLE"]}',
                    '{variable["INPUT_TABLE_NAME"]}',
                    '{variable["INPUT_COL_NAME"]}',
                    '{variable["INPUT_FILTERS"]}',
                    '{variable["TO_BE_CHUNKED"]}',
                    '{variable["INPUT_PROCESSING"]}',
                    '{variable["IS_ACTIVE"]}',
                    CURRENT_TIMESTAMP(),
                    '{username}'
                )
            """
            dc.execute_non_query(insert_query)

        return jsonify({"message": "Prompt model and variables inserted successfully.", "prompt_model_id": model_id}), status.HTTP_200_OK

    except Exception as e:
        print("ERROR: Exception in /add_variable_config:", e)
        traceback.print_exc()
        return jsonify({"error": "Internal server error."}), status.HTTP_500_INTERNAL_SERVER_ERROR

def validate_prompt_request(data):
    try:
        required_keys = [
            "insight_type", "prompt_template", "prompt_params_list",
            "llm_model_name", "temperature", "max_tokens", "verbose", "chunking_params"
        ]
        for k in required_keys:
            if k not in data:
                return False, f"Missing required key: {k}"

        prompt_template = data["prompt_template"]
        if not prompt_template.strip():
            return False, "prompt_template cannot be empty"
        if "{context}" not in prompt_template:
            return False, "prompt_template must contain '{context}'"

        try:
            prompt_params = json.loads(data["prompt_params_list"])
        except json.JSONDecodeError:
            return False, "Invalid JSON in prompt_params_list"
        if "variables" not in prompt_params or not isinstance(prompt_params["variables"], list):
            return False, "'prompt_params_list' must contain a 'variables' list"

        missing_vars = [v for v in prompt_params["variables"] if '{' + v + '}' not in prompt_template]
        if missing_vars:
            return False, f"Missing variables in prompt_template: {', '.join(missing_vars)}"

        try:
            temperature = float(data["temperature"])
            if not (temperature == 0):  
                return False, "temperature must be 0 for better Accuracy"
        except ValueError:
            return False, "temperature must be an Integer"

        try:
            max_tokens = int(data["max_tokens"])
        except ValueError:
            return False, "max_tokens must be an integer"

        verbose_raw = data["verbose"]
        if isinstance(verbose_raw, bool):
            verbose = verbose_raw
        elif isinstance(verbose_raw, str):
            if verbose_raw.lower() in ["true", "1"]:
                verbose = True
            elif verbose_raw.lower() in ["false", "0"]:
                verbose = False
            else:
                return False, "verbose must be a boolean"
        else:
            return False, "verbose must be a boolean"

        try:
            chunking = json.loads(data["chunking_params"])
        except json.JSONDecodeError:
            return False, "Invalid JSON in chunking_params"
        if not isinstance(chunking, dict):
            return False, "chunking_params must be a JSON object"

        if "chunk_abs" in chunking and "chunk_ratio" in chunking:
            return False, "Provide either chunk_abs or chunk_ratio, not both"
        if "chunk_abs" not in chunking and "chunk_ratio" not in chunking:
            return False, "Must provide either chunk_abs or chunk_ratio"
        if "chunk_abs" in chunking and not isinstance(chunking["chunk_abs"], int):
            return False, "chunk_abs must be an integer"
        if "chunk_ratio" in chunking and not isinstance(chunking["chunk_ratio"], float):
            return False, "chunk_ratio must be a float"
        if "overlap" not in chunking or not isinstance(chunking["overlap"], int):
            return False, "'overlap' is required and must be an integer"
        if "chunk_agg_delim" in chunking and not isinstance(chunking["chunk_agg_delim"], str):
            return False, "'chunk_agg_delim' must be a string"

        validated_data = {
            "insight_type": data["insight_type"],
            "prompt_template": prompt_template,
            "prompt_params": prompt_params,
            "llm_model_name": data["llm_model_name"],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "verbose": verbose,
            "chunking": chunking
        }

        return True, validated_data

    except Exception as e:
        return False, str(e)

def insert_prompt_model(validated_data, username):
    query = """SELECT CAST(COALESCE(MAX(INT(prompt_model_id)), 0) + 1 AS STRING) as model_id
               FROM hive_metastore.fieldforce_navigator_deployment.llm_prompt_model_master"""
    df = dc.execute_query(query)
    prompt_model_id = df.iloc[0].get("model_id", 1)

    prompt_hash = hashlib.sha256(validated_data["prompt_template"].encode()).hexdigest()
    entry_time =  datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    insert_query = f"""
    INSERT INTO hive_metastore.fieldforce_navigator_deployment.llm_prompt_model_master
    VALUES (
        '{prompt_model_id}',
        '{validated_data["insight_type"]}',
        '{validated_data["prompt_template"]}',
        '{json.dumps(validated_data["prompt_params"])}',
        '{prompt_hash}',
        '{validated_data["llm_model_name"]}',
        {validated_data["temperature"]},
        {validated_data["max_tokens"]},
        {validated_data["verbose"]},
        timestamp('{entry_time}'),
        '{json.dumps(validated_data["chunking"])}',
        '{username}'
    )
    """
    dc.execute_non_query(insert_query)
    return prompt_model_id

# New LLM Prompt into non default table

@app.route('/add_new_llm_prompt', methods=['POST'])
def add_variable_config_route_non_default():
    try:
        data = request.get_json(force=True)
        print(f"DEBUG: Received data: {data}")

        username = request.headers.get("Username")
        if not username:
            return jsonify({"error": "Missing Username in headers."}), status.HTTP_400_BAD_REQUEST

        # Prompt Model Validations
        is_valid, result = validate_prompt_request(data)
        if not is_valid:
            return jsonify({"error": result}), status.HTTP_400_BAD_REQUEST

        validated_data = result
        model_id = insert_prompt_model(validated_data, username)
        prompt_id = str(model_id)

        # Region, Market, Brand validation
        required_keys = ["REGION", "MARKET", "BRAND"]
        for key in required_keys:
            if key not in data or not str(data[key]).strip():
                return jsonify({"error": f"Missing required field: {key}"}), status.HTTP_400_BAD_REQUEST

        region = str(data["REGION"]).strip()
        market = str(data["MARKET"]).strip()
        brand = str(data["BRAND"]).strip()

        # Insert variables from JSON input
        variables_list = data.get("variables", [])
        if not variables_list or not isinstance(variables_list, list):
            return jsonify({"error": "Missing or invalid 'variables' list in request."}), status.HTTP_400_BAD_REQUEST

        for variable in variables_list:
            required_var_keys = [
                "VARIABLE", "INPUT_TABLE_NAME", "INPUT_COL_NAME", "INPUT_FILTERS",
                "TO_BE_CHUNKED", "INPUT_PROCESSING", "IS_ACTIVE"
            ]
            for key in required_var_keys:
                if key not in variable:
                    return jsonify({"error": f"Missing key '{key}' in variable config."}), status.HTTP_400_BAD_REQUEST

            insert_extended_query = f"""
                INSERT INTO hive_metastore.fieldforce_navigator_deployment.llm_prompt_input_data_config
                (PROMPT_ID, REGION, BRAND, MARKET, VARIABLE, INPUT_TABLE_NAME, INPUT_COL_NAME, INPUT_FILTERS,
                 TO_BE_CHUNKED, ENTRY_TIME, INPUT_PROCESSING, IS_ACTIVE, ADDED_BY)
                VALUES (
                    '{prompt_id}',
                    '{region}',
                    '{brand}',
                    '{market}',
                    '{variable["VARIABLE"]}',
                    '{variable["INPUT_TABLE_NAME"]}',
                    '{variable["INPUT_COL_NAME"]}',
                    '{variable["INPUT_FILTERS"]}',
                    '{variable["TO_BE_CHUNKED"]}',
                    CURRENT_TIMESTAMP(),
                    '{variable["INPUT_PROCESSING"]}',
                    '{variable["IS_ACTIVE"]}',
                    '{username}'
                )
            """
            dc.execute_non_query(insert_extended_query)

        return jsonify({
            "message": "Prompt model and variables inserted successfully.",
            "prompt_model_id": model_id
        }), status.HTTP_200_OK

    except Exception as e:
        print("ERROR: Exception in /add_variable_config:", e)
        traceback.print_exc()
        return jsonify({"error": "Internal server error."}), status.HTTP_500_INTERNAL_SERVER_ERROR




# New Market onboard Route 

'''def validate_required_fields(data, required_fields):
    missing = [f for f in required_fields if f not in data or data[f] is None or str(data[f]).strip() == ""]
    return missing'''

def validate_rmp_unique(region, market, product_id):
    """
    Check if a row already exists in ffn_product_market_control_table with the same REGION, MARKETNAME, PRODUCT_VEEVA_ID
    Returns True if unique (safe to insert), False if already exists.
    """
    try:
        query = f"""
            SELECT *
            FROM hive_metastore.fieldforce_navigator_deployment.ffn_product_market_control_table
            WHERE REGION = '{region}'
              AND MARKETNAME = '{market}'
              AND PRODUCT_VEEVA_ID = '{product_id}'
        """
        df = dc.execute_query(query)
        return len(df) == 0
    except Exception as e:
        print("Error validating RMP uniqueness:", e)
        raise

def validate_audio_translation_uniqueness(region, market, brand):
   
    try:
        query = f"""
            SELECT *
            FROM hive_metastore.fieldforce_navigator_deployment.audio_translation_config
            WHERE REGION = '{region}'
              AND MARKET = '{market}'
              AND BRAND = '{brand}'
        """
        df = dc.execute_query(query)
        return len(df) == 0
    except Exception as e:
        print("Error validating audio_translation uniqueness:", e)
        raise

def validate_email_unique(region, market, brand):
   
    try:
        query = f"""
            SELECT *
            FROM hive_metastore.fieldforce_navigator_deployment.audio_email_monitoring_config
            WHERE REGION = '{region}'
              AND MARKET = '{market}'
              AND BRAND = '{brand}'
        """
        df = dc.execute_query(query)
        return len(df) == 0
    except Exception as e:
        print("Error validating Email uniqueness:", e)
        raise

'''def validate_rmb_unique(region, market, brand):
    """
    Check if a row already exists in audio_market_brand_config with the same REGION, MARKET, BRAND
    Returns True if unique (safe to insert), False if already exists.
    """
    try:
        query = f"""
            SELECT *
            FROM hive_metastore.fieldforce_navigator_deployment.audio_market_brand_config
            WHERE REGION = '{region}'
              AND MARKET = '{market}'
              AND BRAND = '{brand}'
        """
    
        df = dc.execute_query(query)
        result = len(df) == 0
        return result

    except Exception as e:
        print("Error validating RMB uniqueness:", e)
        raise '''

def insert_ffn_product_control_entry(region, market, product_id, brand_name, sales_flag, calls_flag, username):
    entry_time = datetime.datetime.utcnow().isoformat() + "Z"
    insert_query = f"""
        INSERT INTO hive_metastore.fieldforce_navigator_deployment.ffn_product_market_control_table (
            REGION, MARKETNAME, PRODUCT_VEEVA_ID,
            GLOBAL_BRAND_NAME, SALES_ACTIVE_FLAG, CALLS_ACTIVE_FLAG,
            ADDED_BY, ENTRY_TIME
        )
        VALUES (
            '{region}', '{market}', '{product_id}',
            '{brand_name}', '{sales_flag}', '{calls_flag}',
            '{username}', '{entry_time}'
        )
    """
    dc.execute_non_query(insert_query)



def insert_audio_email_monitor_config(region, market, brand,
                                     email_to, email_cc, email_subject, username):
    """
    Inserts a new record into audio_email_monitoring_config.
    """
    entry_time = datetime.datetime.utcnow().isoformat() + "Z"
    insert_query = f"""
        INSERT INTO hive_metastore.fieldforce_navigator_deployment.audio_email_monitoring_config (
            REGION, MARKET, BRAND, EMAIL_TO, EMAIL_CC, SUBJECT_NAME, ADDED_BY, ENTRY_TIME
        )
        VALUES (
            '{region}', '{market}', '{brand}',
            '{email_to}', '{email_cc}', '{email_subject}',
            '{username}', '{entry_time}'
            
        )
    """
    dc.execute_non_query(insert_query) 



def insert_audio_translation_config(region, market, brand,username):
    """
    Inserts a new record into insert_audio_translation_config.
    """
    entry_time = datetime.datetime.utcnow().isoformat() + "Z"
    insert_query = f"""
        INSERT INTO hive_metastore.fieldforce_navigator_deployment.audio_translation_config(
            REGION, MARKET, BRAND, WORDS_TO_EXCLUDE_TRANSLATION,ADDED_BY, ENTRY_TIME
        )
        VALUES (
            '{region}', '{market}', '{brand}',
            '{brand}',
            '{username}', '{entry_time}'
            
        )
    """
    dc.execute_non_query(insert_query) 



def insert_audio_market_brand_config_new(region, market, brand,
                                     transcript_lang, pii_lang, translation_lang,
                                     llm_default_prompt_flag, language,
                                     username,
                                     no_of_archival_days=10,
                                     bypass_llm_translation_flag=True,
                                     no_of_past_summaries=4):
    """
    Inserts a new record into audio_market_brand_config.
    """
    entry_time = datetime.datetime.utcnow().isoformat() + "Z"
    insert_query = f"""
        INSERT INTO hive_metastore.fieldforce_navigator_deployment.audio_market_brand_config (
            REGION, MARKET, BRAND,
            TRANSCRIPT_LANGUAGE, PII_REMOVAL_LANGUAGE, TRANSLATION_LANGUAGE,
            NO_OF_ARCHIVAL_DAYS, BYPASS_LLM_TRANSLATION_FLAG, ADDED_BY, ENTRY_TIME,
            LLM_DEFAULT_PROMPT_FLAG, LANGUAGE, NO_OF_PAST_SUMMARIES
        )
        VALUES (
            '{region}', '{market}', '{brand}',
            '{transcript_lang}', '{pii_lang}', '{translation_lang}',
            {no_of_archival_days}, {str(bypass_llm_translation_flag).lower()}, '{username}', '{entry_time}',
            {str(llm_default_prompt_flag).lower()}, '{language}', {no_of_past_summaries}
        )
    """
    dc.execute_non_query(insert_query) 


def delete_ffn_product_control_entry(region, market, product_id):
    delete_query = f"""
        DELETE FROM hive_metastore.fieldforce_navigator_deployment.ffn_product_market_control_table
        WHERE REGION = '{region}'
          AND MARKETNAME = '{market}'
          AND PRODUCT_VEEVA_ID = '{product_id}'
    """
    dc.execute_non_query(delete_query)


def delete_audio_email_monitor_config(region, market, brand):
    delete_query = f"""
        DELETE FROM hive_metastore.fieldforce_navigator_deployment.audio_email_monitoring_config
        WHERE REGION = '{region}'
          AND MARKET = '{market}'
          AND BRAND = '{brand}'
    """
    dc.execute_non_query(delete_query)


def delete_audio_market_brand_config(region, market, brand):
    delete_query = f"""
        DELETE FROM hive_metastore.fieldforce_navigator_deployment.audio_market_brand_config
        WHERE REGION = '{region}'
          AND MARKET = '{market}'
          AND BRAND = '{brand}'
    """
    dc.execute_non_query(delete_query)



@app.route('/new_market_onboard', methods=['POST'])
def add_ffn_product_control_entry():
    try:
        data = request.get_json(force=True, silent=True) or {}
        username = request.headers.get("Username")

        if not username:
            return jsonify({"error": "Missing 'Username' in headers."}), status.HTTP_400_BAD_REQUEST

        # Validate unified required fields
        required_fields = [
            "REGION", "MARKET", "PRODUCT_VEEVA_ID", "BRAND",
            "SALES_ACTIVE_FLAG", "CALLS_ACTIVE_FLAG",
            "TRANSCRIPT_LANGUAGE", "PII_REMOVAL_LANGUAGE", "TRANSLATION_LANGUAGE",
            "LLM_DEFAULT_PROMPT_FLAG", "LANGUAGE","EMAIL_TO","EMAIL_CC","SUBJECT_NAME"
        ]
        missing_fields = validate_required_fields(data, required_fields)
        if missing_fields:
            return jsonify({"error": f"Missing required fields: {', '.join(missing_fields)}"}), status.HTTP_400_BAD_REQUEST

        region = str(data["REGION"]).strip()
        market = str(data["MARKET"]).strip()
        product_id = str(data["PRODUCT_VEEVA_ID"]).strip()
        brand = str(data["BRAND"]).strip()
        sales_flag = str(data["SALES_ACTIVE_FLAG"]).strip().lower()
        calls_flag = str(data["CALLS_ACTIVE_FLAG"]).strip().lower()

        transcript_lang = str(data["TRANSCRIPT_LANGUAGE"]).strip()
        pii_lang = str(data["PII_REMOVAL_LANGUAGE"]).strip()
        translation_lang = str(data["TRANSLATION_LANGUAGE"]).strip()
        llm_default_prompt_flag = str(data["LLM_DEFAULT_PROMPT_FLAG"]).strip().lower() in ("true", "1", "yes")
        language = str(data["LANGUAGE"]).strip()
        email_to = str(data["EMAIL_TO"]).strip()
        email_cc = str(data["EMAIL_CC"]).strip()
        email_subject = str(data["SUBJECT_NAME"]).strip()
        validations_passed = True

        no_of_archival_days = int(data.get("NO_OF_ARCHIVAL_DAYS", 10))
        bypass_llm_translation_flag = str(data.get("BYPASS_LLM_TRANSLATION_FLAG", "true")).strip().lower() in ("true", "1", "yes")
        no_of_past_summaries = int(data.get("NO_OF_PAST_SUMMARIES", 4))

        if sales_flag not in ("true", "false"):
            return jsonify({"error": "SALES_ACTIVE_FLAG must be 'true' or 'false'"}), status.HTTP_400_BAD_REQUEST
        if calls_flag not in ("true", "false"):
            return jsonify({"error": "CALLS_ACTIVE_FLAG must be 'true' or 'false'"}), status.HTTP_400_BAD_REQUEST
        

        if not validate_rmp_unique(region, market, product_id):
            validations_passed = False
            return jsonify({"error": f"Entry already exists for {region}-{market}-{product_id}."}), status.HTTP_400_BAD_REQUEST


        if not validate_rmb_unique(region, market, brand):
            validations_passed = False
            return jsonify({"error": f"Configuration already exists for {region}-{market}-{brand}. in audio_market_brand_config"}), status.HTTP_400_BAD_REQUEST

        if not validate_email_unique(region, market, brand):
            validations_passed = False
            return jsonify({"error": f"Configuration already exists for {region}-{market}-{brand}. in audio_email_monitoring_config"}), status.HTTP_400_BAD_REQUEST


        if not validate_audio_translation_uniqueness(region, market, brand):
            validations_passed = False
            return jsonify({"error": f"Configuration already exists for {region}-{market}-{brand}. in audio_translation_config"}), status.HTTP_400_BAD_REQUEST

        if validations_passed:
            insert_ffn_product_control_entry(
            region=region,
            market=market,
            product_id=product_id,
            brand_name=brand,
            sales_flag=sales_flag,
            calls_flag=calls_flag,
            username=username
            )
            insert_audio_market_brand_config_new(
            region=region,
            market=market,
            brand=brand,
            transcript_lang=transcript_lang,
            pii_lang=pii_lang,
            translation_lang=translation_lang,
            llm_default_prompt_flag=llm_default_prompt_flag,
            language=language,
            username=username,
            no_of_archival_days=no_of_archival_days,
            bypass_llm_translation_flag=bypass_llm_translation_flag,
            no_of_past_summaries=no_of_past_summaries
            )
            insert_audio_email_monitor_config(
            region=region,
            market=market,
            brand=brand,
            email_to=email_to, 
            email_cc=email_cc, 
            email_subject=email_subject, 
            username=username)
            insert_audio_translation_config(
            region=region,
            market=market,
            brand=brand,
            username=username)
            return jsonify({"message": f"Entries added in All Tables for {region}-{market}-{brand}"}), status.HTTP_201_CREATED

    except Exception as e:
        print("ERROR in add_ffn_product_control_entry:", e)
        traceback.print_exc()
        return jsonify({"error": "Internal server error."}), status.HTTP_500_INTERNAL_SERVER_ERROR


# ===== Route : LLM Medical Words Examples =====
# Table: hive_metastore.fieldforce_navigator_deployment.llm_medical_words_examples
# Columns: BRAND, INCORRECT_TERM, CORRECTED_TERM, ADDED_BY, ENTRY_TIME
 


# ===== Route: LLM Medical Words Examples (insert) =====
# Table: hive_metastore.fieldforce_navigator_deployment.llm_medical_words_examples
# Columns: BRAND, INCORRECT_TERM, CORRECTED_TERM, ADDED_BY, ENTRY_TIME
@app.route('/llm_mwe_table', methods=['POST'])
def add_llm_medical_word_example():
    try:
        data = request.get_json(force=True, silent=True) or {}

        # who added it? prefer payload > header (keeps behavior similar to rest of API)
        added_by = str(data.get("ADDED_BY", "")).strip() or str(request.headers.get("Username", "")).strip()
        if not added_by:
            return jsonify({"error": "Missing 'ADDED_BY' in body or 'Username' in headers."}), status.HTTP_400_BAD_REQUEST

        # basic required fields
        required = ["BRAND", "INCORRECT_TERM", "CORRECTED_TERM"]
        missing = [k for k in required if k not in data or str(data.get(k, "")).strip() == ""]
        if missing:
            return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), status.HTTP_400_BAD_REQUEST

        # tiny normalizer: trim + collapse spaces
        def _clean(x):
            return " ".join(str(x).strip().split())

        brand = _clean(data["BRAND"])
        incorrect = _clean(data["INCORRECT_TERM"])
        corrected = _clean(data["CORRECTED_TERM"])

        # check dup (case-insensitive on all three) — same style as rest of codebase
        exists_sql = f"""
            SELECT 1
            FROM hive_metastore.fieldforce_navigator_deployment.llm_medical_words_examples
            WHERE UPPER(TRIM(BRAND)) = UPPER(TRIM('{brand.replace("'", "''")}'))
              AND UPPER(TRIM(INCORRECT_TERM)) = UPPER(TRIM('{incorrect.replace("'", "''")}'))
              AND UPPER(TRIM(CORRECTED_TERM)) = UPPER(TRIM('{corrected.replace("'", "''")}'))
            LIMIT 1
        """
        df = dc.execute_query(exists_sql)
        if df is not None and getattr(df, "empty", True) is False:
            return jsonify({
                "status": "error",
                "message": "Record already existing (case-insensitive).",
                "conflict_on": {
                    "BRAND": brand,
                    "INCORRECT_TERM": incorrect,
                    "CORRECTED_TERM": corrected
                }
            }), status.HTTP_409_CONFLICT

        # insert (server-side timestamp)
        insert_sql = f"""
            INSERT INTO hive_metastore.fieldforce_navigator_deployment.llm_medical_words_examples
                (BRAND, INCORRECT_TERM, CORRECTED_TERM, ADDED_BY, ENTRY_TIME)
            VALUES
                ('{brand.replace("'", "''")}',
                 '{incorrect.replace("'", "''")}',
                 '{corrected.replace("'", "''")}',
                 '{added_by.replace("'", "''")}',
                 CURRENT_TIMESTAMP())
        """
        dc.execute_non_query(insert_sql)

        return jsonify({
            "status": "success",
            "message": "Record inserted.",
            "data": {
                "BRAND": brand,
                "INCORRECT_TERM": incorrect,
                "CORRECTED_TERM": corrected,
                "ADDED_BY": added_by
            }
        }), status.HTTP_201_CREATED

    except Exception as e:
        # keep error style consistent with project
        return jsonify({"error": f"Internal server error: {str(e)}"}), status.HTTP_500_INTERNAL_SERVER_ERROR

