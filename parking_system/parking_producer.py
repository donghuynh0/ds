import time
import random
import json
from datetime import datetime
from enum import Enum
from kafka import KafkaProducer


class ParkingStatus(Enum):
    """Parking status of a vehicle"""
    ENTERING = "Entering"
    PARKED = "Parked"
    MOVING = "Moving"
    EXITING = "Exiting"


class ParkingEvent:
    """Class representing a parking event"""

    LICENSE_PLATES = [
        "29A-12345", "29A-54321", "29A-67890", "29A-11111", "29A-99999",
        "30B-12345", "30B-67890", "30B-33333", "30B-88888", "30B-55555",
        "51C-11111", "51C-22222", "51C-44444", "51C-77777", "51C-12121",
        "59D-98765", "59D-45678", "59D-13579", "59D-24680", "59D-86420",
        "79D-99999", "79D-10101", "79D-20202", "79D-30303", "79D-40404",
        "92E-54321", "92E-65432", "92E-76543", "92E-87654", "92E-98765",
        "15F-88888", "15F-11122", "15F-33344", "15F-55566", "15F-77788",
        "43G-22222", "43G-12389", "43G-45612", "43G-78945", "43G-32165",
        "60H-10203", "60H-40506", "60H-70809", "60H-20406", "60H-50810"
    ]

    PARKING_LOCATIONS = [
        # Floor A
        "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10",
        # Floor B
        "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9", "B10",
        # Floor C
        "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "C10",
        # Floor D
        "D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9", "D10",
        # Floor E
        "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9", "E10",
        # Floor F (VIP)
        "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10"
    ]

    def __init__(self, occupied_locations=None, active_license_plates=None, entry_time=None):
        # Choose a license plate not currently active
        if active_license_plates:
            available_plates = [plate for plate in self.LICENSE_PLATES if plate not in active_license_plates]
            if available_plates:
                self.license_plate = random.choice(available_plates)
            else:
                self.license_plate = random.choice(self.LICENSE_PLATES)
        else:
            self.license_plate = random.choice(self.LICENSE_PLATES)

        # Choose an unoccupied location
        if occupied_locations:
            available_locations = [loc for loc in self.PARKING_LOCATIONS if loc not in occupied_locations]
            if available_locations:
                self.location = random.choice(available_locations)
            else:
                self.location = random.choice(self.PARKING_LOCATIONS)
        else:
            self.location = random.choice(self.PARKING_LOCATIONS)

        self.status = ParkingStatus.ENTERING
        self.parked_count = 0
        self.parked_duration = 0
        self.entry_timestamp = entry_time if entry_time else int(time.time())

    def next_status(self, occupied_locations=None, active_license_plates=None):
        """Transition to the next status logically"""
        if self.status == ParkingStatus.ENTERING:
            self.status = ParkingStatus.PARKED
            self.parked_duration = random.randint(20, 200)
            self.parked_count = 0

        elif self.status == ParkingStatus.PARKED:
            self.parked_count += 1
            if self.parked_count >= self.parked_duration:
                self.status = ParkingStatus.MOVING

        elif self.status == ParkingStatus.MOVING:
            self.status = ParkingStatus.EXITING

        else:
            # Once exited, create a new vehicle with available plate/location
            current_time = int(time.time())
            self.__init__(occupied_locations, active_license_plates, current_time)

    def get_event_info(self):
        """Return event information as a dictionary"""
        current_timestamp = int(time.time())
        return {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp_unix": current_timestamp,
            "license_plate": self.license_plate,
            "location": self.location,
            "status_code": self.status.name,
            "entry_timestamp": self.entry_timestamp
        }


def parking_stream_to_kafka(
    kafka_bootstrap_servers,
    kafka_topic,
    duration_minutes=30,
    event_interval=3
):

    # Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )

    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)

    # Track occupied locations and active plates
    occupied_locations = set()
    active_license_plates = set()

    # Create initial vehicles to simulate real parking
    active_vehicles = []
    for _ in range(5):
        vehicle = ParkingEvent(occupied_locations, active_license_plates)
        active_vehicles.append(vehicle)
        occupied_locations.add(vehicle.location)
        active_license_plates.add(vehicle.license_plate)

    try:
        print(f"Starting to stream to Kafka topic: {kafka_topic}")
        print(f"Kafka server: {kafka_bootstrap_servers}")
        print("-" * 60)

        while time.time() < end_time:
            # Randomly pick a vehicle to update
            vehicle = random.choice(active_vehicles)

            # Keep old data
            old_status = vehicle.status
            old_location = vehicle.location
            old_license_plate = vehicle.license_plate

            # Get event info
            event_data = vehicle.get_event_info()

            # Send to Kafka (key = license_plate)
            producer.send(
                kafka_topic,
                key=event_data['license_plate'],
                value=event_data
            )

            # Print to console
            print(json.dumps(event_data, ensure_ascii=False))

            # Move to next status
            vehicle.next_status(occupied_locations, active_license_plates)

            # Manage occupied and active sets
            if old_status == ParkingStatus.EXITING and vehicle.status == ParkingStatus.ENTERING:
                occupied_locations.discard(old_location)
                occupied_locations.add(vehicle.location)
                active_license_plates.discard(old_license_plate)
                active_license_plates.add(vehicle.license_plate)
            elif vehicle.status == ParkingStatus.EXITING and old_status != ParkingStatus.EXITING:
                occupied_locations.discard(vehicle.location)

            # Possibly add a new vehicle
            if random.random() > 0.6 and len(active_vehicles) < 20:
                if (len(occupied_locations) < len(ParkingEvent.PARKING_LOCATIONS) and
                    len(active_license_plates) < len(ParkingEvent.LICENSE_PLATES)):
                    new_vehicle = ParkingEvent(occupied_locations, active_license_plates)
                    active_vehicles.append(new_vehicle)
                    occupied_locations.add(new_vehicle.location)
                    active_license_plates.add(new_vehicle.license_plate)

            # Possibly remove exited vehicles
            if random.random() > 0.5:
                vehicles_to_remove = [v for v in active_vehicles if v.status == ParkingStatus.EXITING]
                for v in vehicles_to_remove:
                    active_vehicles.remove(v)
                    occupied_locations.discard(v.location)
                    active_license_plates.discard(v.license_plate)

            # Ensure at least 3 vehicles are active
            while (len(active_vehicles) < 3 and
                   len(occupied_locations) < len(ParkingEvent.PARKING_LOCATIONS) and
                   len(active_license_plates) < len(ParkingEvent.LICENSE_PLATES)):
                new_vehicle = ParkingEvent(occupied_locations, active_license_plates)
                active_vehicles.append(new_vehicle)
                occupied_locations.add(new_vehicle.location)
                active_license_plates.add(new_vehicle.license_plate)

            # Random delay between events
            delay = random.uniform(event_interval * 0.5, event_interval * 1.5)
            time.sleep(delay)

    except KeyboardInterrupt:
        print("\nStopping stream...")
    finally:
        producer.flush()
        producer.close()
        print("Kafka Producer closed")


if __name__ == "__main__":
    # Kafka settings
    KAFKA_BOOTSTRAP_SERVERS = '192.168.1.56:9092'
    KAFKA_TOPIC = 'parking-events'

    # Stream for 30 minutes with 3s intervals
    parking_stream_to_kafka(
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        kafka_topic=KAFKA_TOPIC,
        duration_minutes=30,
        event_interval=3
    )
