package main

import (
	"errors"
	"net/url"
	"os"
)

type config struct{ InfluxURL, QueryURL, Database, Token, HTTPAddr string }

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func loadConfig() (config, error) {
	writeURL := env("INFLUXDB_URL", "http://influxdb3:8181")
	c := config{
		InfluxURL: writeURL,
		QueryURL:  env("INFLUXDB_QUERY_URL", writeURL),
		Database:  env("INFLUXDB_DATABASE", "logs"),
		Token:     os.Getenv("INFLUXDB_TOKEN"),
		HTTPAddr:  env("HTTP_ADDR", ":8080"),
	}
	for name, rawURL := range map[string]string{"INFLUXDB_URL": c.InfluxURL, "INFLUXDB_QUERY_URL": c.QueryURL} {
		u, err := url.Parse(rawURL)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return config{}, errors.New(name + " must be an absolute HTTP URL")
		}
	}
	return c, nil
}
