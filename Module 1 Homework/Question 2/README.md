# Question 2: Understanding Docker networking and docker-compose

I created a local `docker-compose.yaml` file mirroring the configuration provided in the question:

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - "5433:5432"  # Host Port: 5433, Container Port: 5432
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin

volumes:
  vol-pgdata:
  vol-pgadmin_data:
 ```

 Then I started the services:

 ```bash
 docker compose up -d
 ```


After that, I accessed pgAdmin at http://localhost:8080.
Logged in with credentials: pgadmin@pgadmin.com / pgadmin.
Attempted to register a new server to test the connection.
And with the port 5432 I had a successful result adding a new server.