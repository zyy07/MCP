import logging
import os


from mysql.connector import connect, Error
from mcp.server import  FastMCP
from mcp.types import TextContent
from config.db_config import DBConfig
from database_env import DataBaseEnv
from datasource.db_source import HITLSQLDatabase
from utils.db_util import init_db_conn
from utils.file_util import extract_sql_from_qwen
from utils.llm_util import call_dashscope

mcp = FastMCP("xiyan")


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("xiyan_mcp_server")

def get_model_config():
    model_config ={
        "name":os.getenv("MODEL_NAME","qwen-max-0125"),
        "key":os.getenv("MODEL_KEY",""),
        "url":os.getenv("MODEL_URL","https://dashscope.aliyuncs.com/compatible-mode/v1")
    }
    if not all([model_config["name"], model_config["key"]]):
        logger.error("Missing required model configuration. Please check environment variables:")
        logger.error("MODEL_NAME and MODEL_KEY are required")
        raise ValueError("Missing required model configuration")

    return model_config

def get_db_config():

    """Get database configuration from environment variables."""
    config = {
        "host": os.getenv("MYSQL_HOST", ""),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER",''),
        "password": os.getenv("MYSQL_PASSWORD",""),
        "database": os.getenv("MYSQL_DATABASE",'')
    }
    
    if not all([config["user"], config["password"], config["database"]]):
        logger.error("Missing required database configuration. Please check environment variables:")
        logger.error("MYSQL_USER, MYSQL_PASSWORD, and MYSQL_DATABASE are required")
        raise ValueError("Missing required database configuration")
    
    return config

def get_xiyan_config(db_config):
    xiyan_db_config = DBConfig(dialect='mysql',db_name=db_config['database'], user_name=db_config['user'], db_pwd=db_config['password'], db_host=db_config['host'], port=db_config['port'])
    return xiyan_db_config

global_db_config = get_db_config()
global_xiyan_db_config = get_xiyan_config(global_db_config)
model_config = get_model_config()
model_name= model_config['name']
model_key=model_config['key']
model_url = model_config['url']


@mcp.resource('mysql://'+global_db_config['database'])
async def read_resource() -> str:

    db_engine = init_db_conn(global_xiyan_db_config)
    db_source = HITLSQLDatabase(db_engine)
    return db_source.mschema.to_mschema()

@mcp.resource("mysql://{table_name}")
async def read_resource(table_name) -> str:
    """Read table contents."""
    config = get_db_config()
    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 100")
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                result = [",".join(map(str, row)) for row in rows]
                return "\n".join([",".join(columns)] + result)
                
    except Error as e:
        raise RuntimeError(f"Database error: {str(e)}")


def sql_gen_and_execute(db_env, query: str):
    """
    Transfers the input natural language question to sql query (known as Text-to-sql) and executes it on the database.
     Args:
        query: natural language to query the database. e.g. 查询在2024年每个月，卡宴的各经销商销量分别是多少
    """

    #db_env = context_variables.get('db_env', None)
    prompt = f"""你现在是一名{db_env.dialect}数据分析专家，你的任务是根据参考的数据库schema和用户的问题，编写正确的SQL来回答用户的问题，生成的SQL用```sql 和```包围起来。
【数据库schema】
{db_env.mschema_str}

【问题】
{query}
"""
    #logger.info(f"SQL generation prompt: {prompt}")

    messages = [
        {"role": "system", "content": prompt},
        {"role": "user", "content": f"用户的问题是: {query}"}
    ]
    param = {"model": model_config['name'], "messages": messages,"key":model_key,"url":model_config['url']}

    try:
        response = call_dashscope(**param)
        content = response.choices[0].message.content
        sql_query = extract_sql_from_qwen(content)
        status, res = db_env.database.fetch(sql_query)
        if not status:
            for idx in range(3):
                sql_query = sql_fix(db_env.dialect, db_env.mschema_str, query, sql_query, res)
                status, res = db_env.database.fetch(sql_query)
                if status:
                    break

        sql_res = db_env.database.fetch_truncated(sql_query,max_rows=100)
        markdown_res = db_env.database.trunc_result_to_markdown(sql_res)
        logger.info(f"SQL query: {sql_query}\nSQL result: {markdown_res}")
        return markdown_res

    except Exception as e:
        return str(e)


def sql_fix(dialect: str, mschema: str, query: str, sql_query: str, error_info: str):
    system_prompt = '''现在你是一个{dialect}数据分析专家，需要阅读一个客户的问题，参考的数据库schema，该问题对应的待检查SQL，以及执行该SQL时数据库返回的语法错误，请你仅针对其中的语法错误进行修复，输出修复后的SQL。
注意：
1、仅修复语法错误，不允许改变SQL的逻辑。
2、生成的SQL用```sql 和```包围起来。

【数据库schema】
{schema}
'''.format(dialect=dialect, schema=mschema)
    user_prompt = '''【问题】
{question}

【待检查SQL】
{sql}

【错误信息】
{sql_res}'''.format(question=query, sql=sql_query, sql_res=error_info)

    model = model_name
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]
    param = {"model": model, "messages": messages,"key":model_key,'url':model_config['url']}

    response = call_dashscope(**param)
    content = response.choices[0].message.content
    sql_query = extract_sql_from_qwen(content)

    return sql_query

def call_xiyan(query: str)-> str:
    """Fetch the data from database through a natural language query

    Args:
        query: The query in natual language
    """
    db_config = get_db_config()
    xiyan_config = get_xiyan_config(db_config)

    logger.info(f"Calling tool with arguments: {query}")
    try:
        db_engine = init_db_conn(xiyan_config)
        db_source = HITLSQLDatabase(db_engine)
    except Exception as  e:

        return "数据库连接失败"+str(e)
    logger.info(f"Calling xiyan")
    env = DataBaseEnv(db_source)
    res = sql_gen_and_execute(env,query)

    return str(res)
@mcp.tool()
def get_data_via_natual_language(query: str)-> list[TextContent]:
    """Fetch the data from database through a natural language query

    Args:
        query: The query in natual language
    """

    res=call_xiyan(query)
    return [TextContent(type="text", text=res)]



if __name__ == "__main__":

    mcp.run()

