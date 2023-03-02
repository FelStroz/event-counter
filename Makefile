EXCHANGE=eventcountertest
AMQP_PORT=5672
AMQP_UI_PORT=15672
CURRENT_DIR=$(shell pwd)

## need for all tests
test:
	go clean -testcache
	go test -v ./... -short -consume=true -size=100 -amqp-url="amqp://guest:guest@localhost:$(AMQP_PORT)" -amqp-exchange="$(EXCHANGE)" -amqp-declare-queue=true -lifetime=5

## need to run
env-up:
	docker run -d --name evencountertest-rabbitmq -p $(AMQP_UI_PORT):15672 -p $(AMQP_PORT):5672 rabbitmq:3-management

env-down:
	docker rm -f evencountertest-rabbitmq

build-generator:
	go build -o bin/generator cmd/generator/*.go

build-consumer:
	go build -o bin/consumer cmd/consumer/*.go

generator-publish: build-generator
	bin/generator -publish=true -size=100 -amqp-url="amqp://guest:guest@localhost:$(AMQP_PORT)" -amqp-exchange="$(EXCHANGE)" -amqp-declare-queue=true

generator-publish-with-resume: build-generator
	bin/generator -publish=true -size=100 -amqp-url="amqp://guest:guest@localhost:$(AMQP_PORT)" -amqp-exchange="$(EXCHANGE)" -amqp-declare-queue=true -count=true -count-out="$(CURRENT_DIR)"

generator-consumer: build-consumer
	bin/generator -consume=true -size=100 -amqp-url="amqp://guest:guest@localhost:$(AMQP_PORT)" -amqp-exchange="$(EXCHANGE)" -amqp-declare-queue=true -lifetime=5

generator-consumer-with-resume: build-consumer
	bin/generator -consume=true -size=100 -amqp-url="amqp://guest:guest@localhost:$(AMQP_PORT)" -amqp-exchange="$(EXCHANGE)" -amqp-declare-queue=true -lifetime=5 -count=true -count-out="$(CURRENT_DIR)"
up: build-generator build-consumer env-up




