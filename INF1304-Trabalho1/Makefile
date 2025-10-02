.PHONY: all stop producer consumer build up status frontend clean logs \
	logs-producer logs-consumer logs-kafka logs-frontend

# === Variáveis ===
PRODUCER_DIR=producer-service
CONSUMER_DIR=consumer-service
PRODUCER_JAR=$(PRODUCER_DIR)/target/producer-service-jar-with-dependencies.jar
CONSUMER_JAR=$(CONSUMER_DIR)/target/consumer-service-jar-with-dependencies.jar

# Alvo principal (roda tudo - menos o frontend)
all: stop $(PRODUCER_JAR) $(CONSUMER_JAR) build up status

# Para o que estiver rodando
stop:
	@echo "Parando containers..."
	docker ps -a
	-docker stop consumer-service0 consumer-service1 producer-service0 producer-service1 kafka1 kafka2 kafka3 frontend1
	-docker rm consumer-service0 consumer-service1 producer-service0 producer-service1 kafka1 kafka2 kafka3 frontend1
	docker ps -a

# Compila o produtor
$(PRODUCER_JAR): $(wildcard $(PRODUCER_DIR)/src/main/java/**/*.java) $(PRODUCER_DIR)/pom.xml
	@echo "Compilando Producer..."
	cd $(PRODUCER_DIR) && mvn clean package

# Compila o consumidor
$(CONSUMER_JAR): $(wildcard $(CONSUMER_DIR)/src/main/java/**/*.java) $(CONSUMER_DIR)/pom.xml
	@echo "Compilando Consumer..."
	cd $(CONSUMER_DIR) && mvn clean package

# Constrói as imagens
build: $(PRODUCER_JAR) $(CONSUMER_JAR)
	@echo "Criando as imagens Docker..."
	docker compose build

# Sobe os containers
up:
	@echo "Subindo os containers..."
	docker compose up -d

# Lista os containers
status:
	@echo "Status dos containers:"
	docker ps -a

# Logs dos producers
logs-producer:
	@echo "Mostrando logs dos Producers..."
	docker compose logs -f producer-service0 producer-service1

# Logs dos consumers
logs-consumer:
	@echo "Mostrando logs dos Consumers..."
	docker compose logs -f consumer-service0 consumer-service1

# Logs do cluster Kafka
logs-kafka:
	@echo "Mostrando logs do Kafka..."
	docker compose logs -f kafka1 kafka2 kafka3

# Logs gerais (producer + consumer)
logs-all:
	@echo "Mostrando logs de Producers e Consumers..."
	docker compose logs -f producer-service0 producer-service1 consumer-service0 consumer-service1

# Limpa build do produtor e consumidor
clean:	stop
	@echo "Limpando builds..."
	cd $(PRODUCER_DIR) && mvn clean
	cd $(CONSUMER_DIR) && mvn clean