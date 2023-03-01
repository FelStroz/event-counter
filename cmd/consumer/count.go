package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	eventcounter "github.com/reb-felipe/eventcounter/pkg"
)

func CountMessages(msgs chan eventcounter.MessageConsumed) map[string]map[string]int {
	output := make(map[string]map[string]int)

	for v := range msgs {
		if _, ok := output[v.EventType]; !ok {
			output[v.EventType] = make(map[string]int)
		}
		if _, ok := output[v.EventType][v.User]; !ok {
			output[v.EventType][v.User] = 0
		}
		output[v.EventType][v.User] += 1
	}

	log.Print(output)

	return output
}

func Write(path string, msgs chan eventcounter.MessageConsumed) {
	for i, v := range CountMessages(msgs) {
		if err := createAndWriteFile(path, i, v); err != nil {
			continue
		}
	}
}

func createAndWriteFile(path, name string, content map[string]int) error {
	file, err := os.Create(fmt.Sprintf("%s/%s-consumer.json", path, name))
	if err != nil {
		log.Printf("can't write file %s-consumer.json, err: %s", name, err)
		return err
	}
	defer file.Close()

	b, err := json.MarshalIndent(content, "", "\t")
	if err != nil {
		log.Printf("can't marshal data for file %s-consumer.json, err: %s", name, err)
		return err
	}

	if _, err := file.Write(b); err != nil {
		log.Printf("can't write data for file %s-consumer.json, err: %s", name, err)
		return err
	}

	return nil
}
