package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	eventcounter "github.com/reb-felipe/eventcounter/pkg"
)

func Write(path string, counter map[eventcounter.EventType]map[string]int) error {
	for k, v := range counter {
		file, err := os.Create(fmt.Sprintf("%s/%s-consumer.json", path, k))
		if err != nil {
			log.Printf("can't write file %s-consumer.json, err: %s", k, err)
			return err
		}
		defer file.Close()

		b, err := json.MarshalIndent(v, "", "\t")
		if err != nil {
			log.Printf("can't marshal data for file %s-consumer.json, err: %s", k, err)
			return err
		}

		if _, err := file.Write(b); err != nil {
			log.Printf("can't write data for file %s-consumer.json, err: %s", k, err)
			return err
		}
	}

	return nil
}
