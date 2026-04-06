from dataclasses import dataclass


@dataclass
class TripRecord:
  
    trip_start_time: str
    pickup_location: str
    dropoff_location: str
    fare_amount: float
    trip_distance: float

    def __eq__(self, other: object) -> bool:
       
        if not isinstance(other, TripRecord):
            return False
        return (
            self.trip_start_time == other.trip_start_time and
            self.pickup_location == other.pickup_location and
            self.dropoff_location == other.dropoff_location
        )

    def __hash__(self) -> int:
       
        return hash((
            self.trip_start_time,
            self.pickup_location,
            self.dropoff_location
        ))

    def __repr__(self) -> str:
    
        return (
            f"TripRecord("
            f"{self.trip_start_time} | "
            f"{self.pickup_location} → {self.dropoff_location} | "
            f"${self.fare_amount}"
            f")"
        )