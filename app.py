from flask import Flask, request, jsonify, make_response
from neo4j import GraphDatabase
from dotenv import load_dotenv
from flask_cors import CORS
import logging
import os

load_dotenv()


app = Flask(__name__)
CORS(app)

logging.basicConfig(filename='app.log', level=logging.DEBUG)

uri = os.getenv("NEO4J_URI", "bolt://ec2-54-88-88-30.compute-1.amazonaws.com:7687")
username = os.getenv("NEO4J_USERNAME", "default_username")
password = os.getenv("NEO4J_PASSWORD", "default_password")

def create_watched_relation(tx, user_id, video_id):
    query = (
        "MATCH (u:User), (v:Video) "
        "WHERE u.user_id = $user_id AND v.video_id = $video_id "
        "MERGE (u)-[:WATCHED]->(v)"
    )
    logging.debug(f"Executing query: {query}")
    tx.run(query, user_id=user_id, video_id=video_id)

@app.route('/watched', methods=['POST'])
def watched_video():
    try:
        user_id, user_profile, video_id = (
            request.json.get('user_id'),
            request.json.get('user_profile'),
            request.json.get('video_id')
        )
        logging.debug(f"Received data - user_id: {user_id}, user_profile: {user_profile}, video_id: {video_id}")

        if not all((user_id, user_profile, video_id)):
            return make_response(jsonify({'success': False, 'error': 'Missing required parameters'}), 400)

        with GraphDatabase.driver(uri, auth=(username, password)) as driver:
            with driver.session() as session:
                session.write_transaction(create_watched_relation, user_id, video_id)
        logging.info("Watched video recorded successfully")
        response_data = {
            'success': True,
            'message': 'Watched video recorded successfully',
            'user_id': user_id,
            'video_id': video_id
        }

        return make_response(jsonify(response_data), 201)

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return make_response(jsonify({'success': False, 'error': str(e)}), 500)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9091, debug=True)
