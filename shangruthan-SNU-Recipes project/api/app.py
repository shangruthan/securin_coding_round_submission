from flask import Flask, request, jsonify, g
from functools import wraps
import psycopg2
from psycopg2.extras import RealDictCursor
import re
import os

API_KEY = os.getenv('API_KEY') 

# def regex(args):

def require_api_key(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        api_key = request.headers.get('X-API-Key')
        if api_key and api_key == API_KEY:
            return func(*args, **kwargs)
        else:
             abort(401)  
    return wrapper


def extract_calories(cal_string):
    try:
        return int(re.findall(r'\d+', cal_string)[0])
    except:
        return 0

app = Flask(__name__)

dbname=os.getenv('DB_NAME') or "securin"
user=os.getenv('DB_USER') or "postgres"
password=os.getenv('DB_PASSWORD') or "Shangruthan@05"
host=os.getenv('DB_HOST') or "localhost"
port=os.getenv('DB_PORT') or "5432"

def get_db_connection():
    if 'db_conn' not in g:
        g.db_conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
    return g.db_conn       


@app.route('/api/recipes', methods=['GET'])
def get_all_recipes():
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 10))
    offset = (page - 1) * limit

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) AS total FROM recipes")

    cursor.execute("SELECT * FROM recipes LIMIT %s OFFSET %s", (limit, offset))
    recipes = cursor.fetchall()

    conn.close()
    if page!=0 and limit!=0:
        return jsonify({
                "page": page,
                "limit": limit,
                "data": recipes
            })



app.route('/api/recipes/search', methods=['GET'])
def search_recipes():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    query = "SELECT * FROM recipes WHERE 1=1"
    params = []

    for key, val in request.args.items():
        if key == 'title':
            query += " AND LOWER(title) LIKE %s"
            params.append(f"%{val.lower()}%")
        elif key == 'cuisine':
            query += " AND LOWER(cuisine) LIKE %s"
            params.append(f"%{val.lower()}%")
        elif key == 'rating':
            match = re.match(r'(<=|>=|=|<|>)([\d\.]+)', val)
            if match:
                op, rating_val = match.groups()
                query += f" AND rating {op} %s"
                params.append(float(rating_val))
        elif key == 'total_time':
            match = re.match(r'(<=|>=|=|<|>)(\d+)', val)
            if match:
                op, t_val = match.groups()
                query += f" AND total_time {op} %s"
                params.append(int(t_val))


    if 'calories' in request.args:
        match = re.match(r'(<=|>=|=|<|>)(\d+)', request.args['calories'])
        if match:
            op, cal_val = match.groups()
            cal_val = int(cal_val)

            cursor.execute(query, params)
            all_results = cursor.fetchall()

            def cal_filter(row):
                val = extract_calories(row.get("calories", "0"))
                return eval(f"{val} {op} {cal_val}")

            filtered = list(filter(cal_filter, all_results))
            conn.close()
            return jsonify({"data": filtered})

    cursor.execute(query, params)
    results = cursor.fetchall()
    conn.close()

    return jsonify({"data": results})


if __name__ == '__main__':
    app.run(debug=True)