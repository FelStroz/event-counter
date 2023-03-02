package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	eventcounter "github.com/reb-felipe/eventcounter/pkg"
)

func CountMessages(msgs []*eventcounter.Message) map[eventcounter.EventType]map[string]int {
	output := make(map[eventcounter.EventType]map[string]int)

	for _, v := range msgs {
		if _, ok := output[v.EventType]; !ok {
			output[v.EventType] = make(map[string]int)
		}
		if _, ok := output[v.EventType][v.UserID]; !ok {
			output[v.EventType][v.UserID] = 0
		}
		output[v.EventType][v.UserID] += 1
	}

	return output
}

func Write(path string, msgs []*eventcounter.Message) {
	for i, v := range CountMessages(msgs) {
		if err := createAndWriteFile(path, string(i), v); err != nil {
			continue
		}
	}
}

func createAndWriteFile(path, name string, content map[string]int) error {
	file, err := os.Create(fmt.Sprintf("%s/%s.json", path, name))
	if err != nil {
		log.Printf("can't write file %s.json, err: %s", name, err)
		return err
	}
	defer file.Close()

	b, err := json.MarshalIndent(content, "", "\t")
	if err != nil {
		log.Printf("can't marshal data for file %s.json, err: %s", name, err)
		return err
	}

	if _, err := file.Write(b); err != nil {
		log.Printf("can't write data for file %s.json, err: %s", name, err)
		return err
	}

	return nil
}
