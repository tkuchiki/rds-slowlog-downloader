package rdsdownloader

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
)

type Positions map[string]Position

type Position struct {
	PrevLogfile string `json:"prev_logfile"`
	LastWritten int64  `json:"last_written"`
	Size        int64  `json:"size"`
	Marker      string `json:"marker"`
}

func LoadConfig(filename string) (Positions, error) {
	var p Positions
	var b []byte
	fp, err := os.Open(filename)
	if err != nil {
		return p, err
	}

	b, err = ioutil.ReadAll(fp)
	if err != nil {
		return p, err
	}

	if err != nil {
		return p, err
	}

	json.Unmarshal(b, &p)

	return p, nil
}

func WriteConfig(filename string, data interface{}) error {
	b, jerr := json.MarshalIndent(data, "", "  ")

	if jerr != nil {
		return jerr
	}

	return ioutil.WriteFile(filename, b, 0644)
}

func CmpPosition(p1, p2 Positions) bool {
	return reflect.DeepEqual(p1, p2)
}
