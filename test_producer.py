from producer.detector import SyntheticDetector
from producer.producer import Producer

detector = SyntheticDetector("DET-001")
producer = Producer()

event = detector.generate_event()
producer.publish(event)