# Project1-StockMarket 

## Repo Structure

Project1-StockMarket/
├─ compose.yml                  # Orchestrates API, Kafka, Spark, Postgres, pgAdmin, Kafka UI
├─ .env                        # versions, passwords, ports
├─ Dockerfile
├─ apps/                      # Spark jobs consuming Kafka and writing to Postgres
├─ data/
|
├─ notebooks/                  # For ad-hoc exploration
│  └─ exploration.ipynb
│
└─ README.md                   # Setup + run instructions

## Project Tech Stack and Flow

- Kafka UI → inspect topics/messages.
- API → produces JSON events into Kafka.
- Spark → consumes from Kafka, writes to Postgres.
- Postgres → stores results for analytics.
- pgAdmin → manage Postgres visually.
- Power BI → external (connects to Postgres at localhost:5432).

## Run Container

docker compose down
docker compose build
docker compose up -d 


## Output

- Kafka UI → http://localhost:8082  
- Postgres → localhost:5432 (db market-pulse)
- pgAdmin → http://localhost:5050  (add server host postgres, user/pass admin/admin). add a server 
- Spark live UI shows at http://localhost:8081

## check opened ports
    docker compose ps
    docker compose port postgres 5432
        netstat -ano | findstr :5432

## Tail logs
    docker compose logs -f api
    docker compose logs -f kafka
    docker compose logs -f spark

## Restart a single service after edits
    docker compose up -d --build api
    docker compose restart spark

## Validate compose file: docker compose config

## Confirm Spark has the Kafka & Postgres jars: docker compose exec spark bash -lc 'ls /opt/bitnami/spark/jars | egrep "kafka|postgresql"'

## API says queued but no Kafka messages: Check KAFKA_BOOTSTRAP is kafka:9092 inside the API container and that topic is events


## Power BI
    Connect to PostgreSQL:
        Server: localhost:5434
        Database: market_pulse
        Credentials: user- admin, password- admin