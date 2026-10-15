package main

import "testing"

func TestLoadConfigDefaultsAndEnv(t *testing.T) {
	t.Setenv("INFLUXDB_URL", "http://db:8181")
	t.Setenv("INFLUXDB_QUERY_URL", "http://query:8181")
	t.Setenv("INFLUXDB_TOKEN", "secret")
	t.Setenv("HTTP_ADDR", ":9090")
	c, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if c.InfluxURL != "http://db:8181" || c.QueryURL != "http://query:8181" || c.Database != "logs" || c.Token != "secret" || c.HTTPAddr != ":9090" {
		t.Fatalf("bad config %#v", c)
	}
}

func TestLoadConfigRejectsBadURL(t *testing.T) {
	t.Setenv("INFLUXDB_URL", "://bad")
	if _, err := loadConfig(); err == nil {
		t.Fatal("expected error")
	}
}
