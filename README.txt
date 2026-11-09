Required software:
WSL
Apache Kafka

Use WSL for all terminals.

Set up a virtual environment and install dependencies(project directory):
# 1. Install the venv module if you don't have it (might need sudo)
sudo apt update
sudo apt install python3-venv

# 2. Create a virtual environment named 'venv'
python3 -m venv venv

# 3. Activate the virtual environment
source venv/bin/activate

# 4. Install the required packages
pip install websockets pyspark confluent_kafka spacy (might be missing some)

Start kafka(in kafka directory):
# 1. Generate UUID
bin/kafka-storage.sh random-uuid

# 2. Format
bin/kafka-storage.sh format --standalone -t <your-UUID> -c config/server.properties

# 3. Start
bin/kafka-server-start.sh config/server.properties

Run scripts in separate terminals(project directory):
python producer.py
python namedEntities.py

For testing, create consumers in separate terminals(inside the kafka directory):
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bsky-posts
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic entities-counts
