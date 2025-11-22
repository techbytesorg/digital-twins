import json
import random
import time
import asyncio
import math
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

load_dotenv()

class HVACSimulator:
    def __init__(self, connection_string: str, event_hub_name: str):
        self.connection_string = connection_string
        self.event_hub_name = event_hub_name
        self.client = EventHubProducerClient.from_connection_string(
            connection_string, 
            eventhub_name=event_hub_name
        )
        # Simulation start time for consistent time-based calculations
        self.start_time = time.time()
        
        # Apartment unit configuration
        self.apartment_id = "001"
        self.rooms = ["room1", "room2", "room3"]
        
        # Weather variation parameters
        self.weather_variation = random.uniform(-3, 3)  # Random daily weather effect
        self.weather_change_time = time.time() + random.uniform(3600, 7200)  # Change every 1-2 hours
        
        # Occupancy state (max 2 people across all rooms)
        self.total_occupants = 0
        self.occupancy_state = {room: False for room in self.rooms}
        self.occupancy_change_time = time.time() + random.uniform(1800, 3600)  # Change every 30-60 min
        
        # Room-specific stable properties
        self.room_properties = {
            "room1": {
                "TargetTemperature": 22.0,
                "Mode": "cooling",
                "FanSpeed": 2
            },
            "room2": {
                "TargetTemperature": 21.5,
                "Mode": "heating", 
                "FanSpeed": 1
            },
            "room3": {
                "TargetTemperature": 23.0,
                "Mode": "off",
                "FanSpeed": 0
            }
        }
        
        # Property change timing
        self.property_change_time = time.time() + random.uniform(7200, 14400)  # Every 2-4 hours
        
    def get_time_of_day_factor(self, current_time: float) -> Tuple[float, int]:
        """Get time-based factors for temperature simulation"""
        # Convert to hours since start (0-24 cycle)
        hours_elapsed = (current_time - self.start_time) / 3600
        hour_of_day = int(hours_elapsed % 24)
        
        # Sinusoidal temperature pattern: cooler at night, warmer during day
        time_factor = math.sin(2 * math.pi * (hour_of_day - 6) / 24)
        
        return time_factor, hour_of_day
    
    def get_ambient_conditions(self, current_time: float) -> Dict[str, float]:
        """Generate realistic ambient (outdoor) temperature and humidity"""
        time_factor, hour_of_day = self.get_time_of_day_factor(current_time)
        
        # Update weather variation periodically
        if current_time > self.weather_change_time:
            self.weather_variation = random.uniform(-3, 3)
            self.weather_change_time = current_time + random.uniform(3600, 7200)
        
        # Base outdoor temperature
        base_outdoor_temp = 15.0  # seasonal average
        
        # Daily cycle
        daily_variation = 6.5 * time_factor
        
        # Weather effect
        ambient_temp = base_outdoor_temp + daily_variation + self.weather_variation
        
        base_humidity = 60.0

        # Humidity variation
        humidity_variation = -10 * time_factor + 15 * math.sin(2 * math.pi * (hour_of_day - 3) / 24)
        ambient_humidity = max(30, min(90, base_humidity + humidity_variation + random.uniform(-5, 5)))
        
        return {
            "AmbientTemperature": round(ambient_temp, 1),
            "AmbientHumidity": round(ambient_humidity, 1)
        }
    
    def update_occupancy(self, current_time: float) -> None:
        """Update apartment occupancy patterns based on realistic daily routines"""
        if current_time > self.occupancy_change_time:
            # Get current time info for apartment patterns
            time_factor, hour_of_day = self.get_time_of_day_factor(current_time)
            
            # Determine day of week
            hours_elapsed = (current_time - self.start_time) / 3600
            day_of_simulation = int(hours_elapsed / 24)
            is_weekend = (day_of_simulation % 7) >= 5  # Simulate Sat/Sun as weekend
            
            # Calculate apartment occupancy probability based on realistic patterns
            if is_weekend:
                # Weekend patterns - more home time
                if 8 <= hour_of_day <= 23:  # Active hours
                    base_occupancy_prob = 0.8
                else:  # Sleep hours (24-8)
                    base_occupancy_prob = 0.95
            else:
                # Weekday patterns - work/school schedule
                if 6 <= hour_of_day <= 8:  # Morning routine
                    base_occupancy_prob = 0.9
                elif 8 < hour_of_day < 17:  # Work hours  
                    base_occupancy_prob = 0.1
                elif 17 <= hour_of_day <= 22:  # Evening at home
                    base_occupancy_prob = 0.85
                else:  # Sleep hours
                    base_occupancy_prob = 0.95
            
            # Determine how many people should be home (max 2 for apartment)
            target_occupants = 2 if random.random() < base_occupancy_prob else (1 if random.random() < 0.6 else 0)
            
            # Adjust current occupancy toward target
            if target_occupants > self.total_occupants:
                # Add people to apartment
                people_to_add = target_occupants - self.total_occupants
                available_rooms = [r for r in self.rooms if not self.occupancy_state[r]]
                
                for _ in range(min(people_to_add, len(available_rooms))):
                    # Prioritize rooms based on time of day
                    if 22 <= hour_of_day or hour_of_day <= 6:  # Sleep hours
                        # Prefer first available room for sleeping (could be any bedroom)
                        preferred_room = available_rooms[0] if available_rooms else None
                    elif 17 <= hour_of_day <= 21:  # Evening
                        # Prefer room1 for evening activities if available
                        preferred_room = "room1" if "room1" in available_rooms else None
                    else:
                        preferred_room = None
                    
                    if preferred_room and preferred_room in available_rooms:
                        room = preferred_room
                    else:
                        room = random.choice(available_rooms)
                    
                    self.occupancy_state[room] = True
                    available_rooms.remove(room)
                    self.total_occupants += 1
                    
            elif target_occupants < self.total_occupants:
                # Remove people from apartment
                people_to_remove = self.total_occupants - target_occupants
                occupied_rooms = [r for r in self.rooms if self.occupancy_state[r]]
                
                for _ in range(min(people_to_remove, len(occupied_rooms))):
                    room = random.choice(occupied_rooms)
                    self.occupancy_state[room] = False
                    occupied_rooms.remove(room)
                    self.total_occupants -= 1
            
            # People might move between rooms (but total stays same)
            if self.total_occupants > 0 and random.random() < 0.3:  # 30% chance to move
                occupied_rooms = [r for r in self.rooms if self.occupancy_state[r]]
                available_rooms = [r for r in self.rooms if not self.occupancy_state[r]]
                
                if occupied_rooms and available_rooms:
                    # Move one person to different room
                    from_room = random.choice(occupied_rooms)
                    to_room = random.choice(available_rooms)
                    self.occupancy_state[from_room] = False
                    self.occupancy_state[to_room] = True
                    
            # Schedule next occupancy change
            self.occupancy_change_time = current_time + random.uniform(2700, 5400)  # 45-90 minutes
            
    def update_room_properties(self, current_time: float) -> None:
        """Update room properties based on apartment daily routines and energy schedules"""
        if current_time > self.property_change_time:
            # Get current time info for apartment scheduling
            time_factor, hour_of_day = self.get_time_of_day_factor(current_time)
            
            # Determine day of week for weekend vs weekday patterns
            hours_elapsed = (current_time - self.start_time) / 3600
            day_of_simulation = int(hours_elapsed / 24)
            is_weekend = (day_of_simulation % 7) >= 5  # Simulate Sat/Sun as weekend
            
            # Update target temperatures based on apartment daily schedule
            for room in self.rooms:
                current_target = self.room_properties[room]["TargetTemperature"]
                
                if is_weekend:
                    # Weekend - consistent comfort (people home more)
                    if 22 <= hour_of_day or hour_of_day <= 7:  # Sleep hours
                        new_target = 20.0 + random.uniform(-0.5, 0.5)  # Cooler for sleep
                    else:  # Active hours
                        new_target = 22.0 + random.uniform(-0.5, 0.5)  # Comfort temperature
                else:
                    # Weekday - energy saving schedule
                    if 8 < hour_of_day < 17:  # Work hours - save energy
                        new_target = 19.0 + random.uniform(-0.5, 0.5)  # Energy saving
                    elif 6 <= hour_of_day <= 8:  # Morning routine
                        new_target = 22.0 + random.uniform(-0.5, 0.5)  # Comfort for getting ready
                    elif 17 <= hour_of_day <= 22:  # Evening home
                        new_target = 22.5 + random.uniform(-0.5, 0.5)  # Post-work comfort
                    else:  # Sleep hours
                        new_target = 20.0 + random.uniform(-0.5, 0.5)  # Sleep temperature
                
                # Ensure reasonable temperature bounds
                self.room_properties[room]["TargetTemperature"] = round(max(18.0, min(26.0, new_target)), 1)
            
            # Occasionally change Mode based on season/conditions (less frequent than temperature)
            if random.random() < 0.2:  # 20% chance to change Mode
                room = random.choice(self.rooms)
                
                # Simple seasonal logic (could be enhanced with actual ambient temperature)
                ambient_temp = 15.0 + 6.5 * self.get_time_of_day_factor(current_time)[0]  # Approximate current ambient
                
                if ambient_temp > 25:
                    preferred_mode = "cooling"
                elif ambient_temp < 12:
                    preferred_mode = "heating"
                else:
                    preferred_mode = random.choice(["off", "heating", "cooling"])
                
                self.room_properties[room]["Mode"] = preferred_mode
                
                # Adjust fan speed based on Mode
                if self.room_properties[room]["Mode"] == "off":
                    self.room_properties[room]["FanSpeed"] = 0
                else:
                    self.room_properties[room]["FanSpeed"] = random.choice([1, 2, 3])
                
            # Schedule next property change (apartments adjust less frequently)
            self.property_change_time = current_time + random.uniform(10800, 18000)  # 3-5 hours
            
    def get_room_temperature(self, room_id: str, ambient_temp: float, current_time: float) -> float:
        """Generate realistic room temperature with sinusoidal pattern and ambient influence"""
        time_factor, hour_of_day = self.get_time_of_day_factor(current_time)
        
        # Base indoor temperature - warmer during day, cooler at night
        base_indoor = 21.0 + 2.0 * time_factor  # Varies from 19°C to 23°C
        
        # Ambient temperature influence (indoor follows outdoor trends but dampened)
        ambient_influence = (ambient_temp - 15.0) * 0.3  # 30% of ambient variation
        
        # Room-specific variations
        room_variations = {
            "room1": 0.5,    # Slightly warmer (maybe south-facing)
            "room2": -0.3,   # Slightly cooler 
            "room3": 0.0     # Average
        }
        
        # Occupancy effect (people generate heat)
        occupancy_heat = 1.5 if self.occupancy_state[room_id] else 0
        
        # HVAC effect based on Mode and target
        target_temp = self.room_properties[room_id]["TargetTemperature"]
        Mode = self.room_properties[room_id]["Mode"]
        
        hvac_effect = 0
        current_base_temp = base_indoor + ambient_influence + room_variations.get(room_id, 0) + occupancy_heat
        
        if Mode == "heating":
            # If heating, temperature moves toward target proportionally
            if current_base_temp < target_temp:
                temp_difference = target_temp - current_base_temp
                # More heating effect for larger temperature difference
                hvac_effect = min(temp_difference * 0.3, 2.0) + random.uniform(0.2, 0.8)
        elif Mode == "cooling":
            # If cooling, temperature moves toward target proportionally
            if current_base_temp > target_temp:
                temp_difference = current_base_temp - target_temp
                # More cooling effect for larger temperature difference
                hvac_effect = -min(temp_difference * 0.3, 2.0) - random.uniform(0.2, 0.8)
        # Mode == "off" results in hvac_effect = 0 (no HVAC intervention)
                
        # Combine all factors (avoid double-counting base factors already in current_base_temp)
        final_temp = (current_base_temp + 
                     hvac_effect +
                     random.uniform(-0.3, 0.3))  # Small random variation
        
        return round(max(16.0, min(30.0, final_temp)), 1)
        
    def get_room_humidity(self, room_id: str, ambient_humidity: float) -> float:
        """Generate realistic room humidity"""
        # Indoor humidity typically 10-20% lower than outdoor
        base_indoor_humidity = ambient_humidity - random.uniform(10, 20)
        
        # Occupancy increases humidity
        occupancy_humidity = 5.0 if self.occupancy_state[room_id] else 0
        
        # HVAC can affect humidity (cooling dehumidifies, heating can dry air)
        mode = self.room_properties[room_id]["Mode"]
        hvac_humidity_effect = 0
        
        if mode == "cooling":
            hvac_humidity_effect = -random.uniform(2, 5)  # Cooling dehumidifies
        elif mode == "heating":
            hvac_humidity_effect = -random.uniform(1, 3)  # Heating can dry air
            
        final_humidity = (base_indoor_humidity + 
                         occupancy_humidity + 
                         hvac_humidity_effect +
                         random.uniform(-3, 3))
        
        return round(max(25.0, min(75.0, final_humidity)), 1)
    
    def get_power_consumption(self, room_id: str) -> float:
        """
        Calculate realistic power consumption based on HVAC mode and fan speed
            
        Typical HVAC Power Ranges:
        - Standby/Off: 5-15 W
        - Heating: 800-1200 W (electric heating elements)
        - Cooling: 600-1000 W (compressor + fan)
        - Fan speed affects total consumption
        """
        mode = self.room_properties[room_id]["Mode"]
        fan_speed = self.room_properties[room_id]["FanSpeed"]
        
        if mode == "off":
            # Standby power in WATTS
            return round(random.uniform(5, 15), 1)
            
        # Base power consumption by Mode in WATTS
        base_power = {
            "heating": random.uniform(800, 1200),  # WATTS - Electric heating elements
            "cooling": random.uniform(600, 1000)   # WATTS - Compressor + evaporator fan
        }
        
        mode_power = base_power.get(mode, 10)
        
        # Fan speed multiplier (higher fan = more power)
        fan_multipliers = {0: 0.1, 1: 0.7, 2: 1.0, 3: 1.3}
        fan_multiplier = fan_multipliers.get(fan_speed, 1.0)
        
        total_power = mode_power * fan_multiplier
        
        # Add some variation in WATTS
        total_power += random.uniform(-50, 50)
        
        return round(max(5.0, total_power), 1)  # Return WATTS
        
    async def send_apartment_telemetry(self, ambient_conditions: Dict[str, float]):
        """Send apartment-level telemetry"""
        apartment_event = {
            "EventType": "apartment_summary",
            "Timestamp": datetime.now(timezone(timedelta(hours=-7))).isoformat(),
            "UnitID": self.apartment_id,
            "AmbientTemperature": ambient_conditions["AmbientTemperature"],
            "AmbientHumidity": ambient_conditions["AmbientHumidity"],
            "TotalOccupantCount": self.total_occupants
        }

        try:
            event_data = EventData(json.dumps(apartment_event))
            async with self.client:
                await self.client.send_batch([event_data])
            print(f"[APARTMENT] Ambient: {ambient_conditions['AmbientTemperature']}°C, {ambient_conditions['AmbientHumidity']}%")
        except Exception as e:
            print(f"Error sending apartment telemetry: {e}")
            
    async def send_room_telemetry(self, room_id: str, ambient_conditions: Dict[str, float], current_time: float):
        """Send room-level telemetry"""
        
        current_temp = self.get_room_temperature(room_id, ambient_conditions["AmbientTemperature"], current_time)
        humidity = self.get_room_humidity(room_id, ambient_conditions["AmbientHumidity"])
        power = self.get_power_consumption(room_id)
        
        # Create sensor reading event
        sensor_event = {
            "EventType": "sensor_reading",
            "Timestamp": datetime.now(timezone(timedelta(hours=-7))).isoformat(),
            "UnitID": self.apartment_id,
            "RoomID": room_id,
            "AmbientTemperature": ambient_conditions["AmbientTemperature"],
            "AmbientHumidity": ambient_conditions["AmbientHumidity"],
            "CurrentTemperature": current_temp,
            "TargetTemperature": self.room_properties[room_id]["TargetTemperature"],
            "FanSpeed": self.room_properties[room_id]["FanSpeed"],
            "Humidity": humidity,
            "Occupancy": self.occupancy_state[room_id],
            "PowerConsumption": power,
            "TotalOccupantCount": self.total_occupants
        }
        
        try:
            event_data = EventData(json.dumps(sensor_event))
            async with self.client:
                await self.client.send_batch([event_data])
                
            print(f"[{room_id.upper()}] T:{current_temp}°C, H:{humidity}%, "
                  f"Occ:{self.occupancy_state[room_id]}, P:{power}W, "
                  f"Mode:{self.room_properties[room_id]['Mode']}")
                  
        except Exception as e:
            print(f"Error sending telemetry for {room_id}: {e}")
    
    async def run_simulation(self, duration_minutes: int = 60):
        """Run complete apartment HVAC simulation"""
        print(f"Starting realistic HVAC simulation for {duration_minutes} minutes")
        print(f"Apartment: {self.apartment_id}")
        print(f"Rooms: {', '.join(self.rooms)}")
        print("=" * 70)
        
        end_time = time.time() + (duration_minutes * 60)
        
        while time.time() < end_time:
            current_time = time.time()
            
            try:
                # Update dynamic states
                self.update_occupancy(current_time)
                self.update_room_properties(current_time)
                
                # Get ambient conditions
                ambient_conditions = self.get_ambient_conditions(current_time)
                
                # Send apartment-level telemetry
                await self.send_apartment_telemetry(ambient_conditions)
                
                # Send room-level telemetry for all rooms
                for room_id in self.rooms:
                    await self.send_room_telemetry(room_id, ambient_conditions, current_time)
                
                print(f"Total occupants: {self.total_occupants}")
                print("-" * 50)
                
                # Wait 60 seconds (1 minute) before next reading
                await asyncio.sleep(60)
                
            except KeyboardInterrupt:
                print("\n🛑 Simulation stopped by user")
                break
            except Exception as e:
                print(f"⚠️  Simulation error: {e}")
                await asyncio.sleep(5)
                
        print("✅ Simulation completed successfully")

async def main():
    """Main function to run the HVAC simulation"""
    # Configuration - Replace with your actual values
    connection_string = os.getenv("CONNECTION_STRING")
    event_hub_name = os.getenv("EVENT_HUB_NAME")
    
    # Create simulator
    simulator = HVACSimulator(connection_string, event_hub_name)
    
    # Get simulation duration from user
    try:
        duration = int(input("Enter simulation duration in minutes (default 5): ") or "5")
    except ValueError:
        duration = 5
        
    print(f"\n Starting {duration} minute HVAC simulation...")
    
    # Run simulation
    await simulator.run_simulation(duration_minutes=duration)

if __name__ == "__main__":
    asyncio.run(main())