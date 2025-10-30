# web_server.py
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO
from kafka import KafkaConsumer
import json
import threading
from datetime import datetime
from collections import defaultdict

app = Flask(__name__)
app.config['SECRET_KEY'] = 'parking_lot_secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# Global state
parking_state = {
    'vehicles': {},  # {license_plate: {location, entry_time, status}}
    'locations': {},  # {location: license_plate or None}
    'stats': {
        'total_parked': 0,
        'total_fee': 0,
        'available_spots': 60
    }
}

PARKING_FEE_PER_MINUTE = 1000
TOTAL_SPOTS = 60
KAFKA_SERVER = '192.168.1.56:9092'
KAFKA_TOPIC = 'parking-events'

def calculate_fee(entry_timestamp, current_timestamp):
    """Tính phí đỗ xe"""
    minutes = int((current_timestamp - entry_timestamp) / 60)
    return minutes * PARKING_FEE_PER_MINUTE, minutes

def process_kafka_messages():
    """Đọc messages từ Kafka và cập nhật state"""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True
    )
    
    print(f"✅ Connected to Kafka: {KAFKA_SERVER}")
    
    for message in consumer:
        data = message.value
        license_plate = data['license_plate']
        location = data['location']
        status = data['status_code']
        timestamp = data['timestamp_unix']
        entry_timestamp = data['entry_timestamp']
        
        # Cập nhật state
        if status == 'PARKED':
            parking_state['vehicles'][license_plate] = {
                'location': location,
                'entry_timestamp': entry_timestamp,
                'current_timestamp': timestamp,
                'status': status
            }
            parking_state['locations'][location] = license_plate
            
        elif status == 'EXITING':
            if license_plate in parking_state['vehicles']:
                loc = parking_state['vehicles'][license_plate]['location']
                parking_state['locations'][loc] = None
                del parking_state['vehicles'][license_plate]
        
        # Tính toán stats
        total_parked = len(parking_state['vehicles'])
        total_fee = 0
        
        for plate, info in parking_state['vehicles'].items():
            fee, _ = calculate_fee(info['entry_timestamp'], info['current_timestamp'])
            total_fee += fee
        
        parking_state['stats'] = {
            'total_parked': total_parked,
            'total_fee': total_fee,
            'available_spots': TOTAL_SPOTS - total_parked
        }
        
        # Broadcast to all connected clients
        socketio.emit('update', get_dashboard_data())

def get_dashboard_data():
    """Chuẩn bị dữ liệu để gửi đến frontend"""
    vehicles_list = []
    
    for plate, info in parking_state['vehicles'].items():
        fee, minutes = calculate_fee(info['entry_timestamp'], info['current_timestamp'])
        vehicles_list.append({
            'license_plate': plate,
            'location': info['location'],
            'minutes': minutes,
            'fee': fee
        })
    
    # Sắp xếp theo phí giảm dần
    vehicles_list.sort(key=lambda x: x['fee'], reverse=True)
    
    # Nhóm vị trí theo tầng
    floors = defaultdict(list)
    for location, plate in parking_state['locations'].items():
        if plate:
            floor = location[0]
            floors[floor].append({
                'location': location,
                'license_plate': plate
            })
    
    return {
        'stats': parking_state['stats'],
        'vehicles': vehicles_list,
        'floors': dict(floors),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

@app.route('/')
def index():
    return render_template('dashboardv2.html')

@app.route('/api/data')
def get_data():
    return jsonify(get_dashboard_data())

@socketio.on('connect')
def handle_connect():
    print('Client connected')
    socketio.emit('update', get_dashboard_data())

if __name__ == '__main__':
    # Start Kafka consumer in background thread
    kafka_thread = threading.Thread(target=process_kafka_messages, daemon=True)
    kafka_thread.start()
    
    print("🚀 Starting web server on http://localhost:8000")
    socketio.run(app, host='0.0.0.0', port=8000, debug=False)