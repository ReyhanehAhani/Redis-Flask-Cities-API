from flask import Flask, request, jsonify
import redis

app = Flask(__name__)
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

@app.route('/add_city', methods=['POST'])
def add_city():
    data = request.get_json()
    country_code = data.get('country_code')
    city = data.get('city')
    if not country_code or not city:
        return jsonify({'error': 'Country code and city are required.'}), 400
    added = redis_client.sadd(country_code, city)
    if added:
        return jsonify({'message': 'City added successfully.'}), 201
    else:
        return jsonify({'message': 'City already exists.'}), 200

@app.route('/get_cities/<country_code>', methods=['GET'])
def get_cities(country_code):
    cities = redis_client.smembers(country_code)
    if cities:
        return jsonify(list(cities)), 200
    else:
        return jsonify({'message': 'No cities found for this country code.'}), 404

@app.route('/search/<key>', methods=['GET'])
def search(key):
    values = redis_client.smembers(key)
    if values:
        return jsonify(list(values)), 200
    else:
        return jsonify({'message': 'No values found for this key.'}), 404

if __name__ == '__main__':
    app.run(debug=True)


