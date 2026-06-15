package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"text/template"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/influxdata/influxdb-client-go/api"
	"github.com/influxdata/influxdb-client-go/api/write"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Config struct {
	gasMeterResourceId       string
	electricMeterResourceId  string
	glowmarktUsername        string
	glowmarktPassword        string
	influxEndpoint           string
	influxToken              string
	influxOrg                string
	influxBucket             string
	rabbitmqConnectionString string
}

const (
	// This is the hardcoded identifier that is common for all clients
	ApplicationID = "b0f1b774-a586-4f72-9edd-27ead8aa7a8d"
	BaseURL       = "https://api.glowmarkt.com/api/v0-1"
)

// --- Struc
type AuthRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type AuthResponse struct {
	Valid bool   `json:"valid"`
	Token string `json:"token"`
}

type ReadingsResponse struct {
	Status         string      `json:"status"`
	Name           string      `json:"name"`
	ResourceTypeID string      `json:"resourceTypeId"`
	ResourceID     string      `json:"resourceId"`
	Data           [][]float64 `json:"data"` // Array of [timestamp, value]
	Units          string      `json:"units"`
	Classifier     string      `json:"classifier"`
}

// --- Glormarkt API ---
func Authenticate(username, password string) (string, error) {
	apiURL := fmt.Sprintf("%s/auth", BaseURL)

	payload := AuthRequest{
		Username: username,
		Password: password,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("error marshaling auth payload: %w", err)
	}

	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("error creating auth request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("applicationId", ApplicationID)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error making auth request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("error reading auth response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("auth API returned status %s: %s", resp.Status, string(body))
	}

	var authResp AuthResponse
	if err := json.Unmarshal(body, &authResp); err != nil {
		return "", fmt.Errorf("error parsing auth JSON: %w", err)
	}

	if !authResp.Valid {
		return "", fmt.Errorf("authentication failed: invalid credentials")
	}

	return authResp.Token, nil
}

func GetDailyReadings(token, resourceID, from, to string) (*ReadingsResponse, error) {
	apiURL := fmt.Sprintf("%s/resource/%s/readings", BaseURL, resourceID)

	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating readings request: %w", err)
	}

	q := req.URL.Query()
	q.Add("from", from)
	q.Add("to", to)
	q.Add("period", "P1D") // daily
	q.Add("offset", "0")   // UTC
	q.Add("function", "sum")
	req.URL.RawQuery = q.Encode()

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("applicationId", ApplicationID)
	req.Header.Set("token", token)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {

		return nil, fmt.Errorf("error making readings request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading readings response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("readings API returned status %s: %s", resp.Status, string(body))
	}

	var readings ReadingsResponse
	if err := json.Unmarshal(body, &readings); err != nil {
		return nil, fmt.Errorf("error parsing readings JSON: %w", err)
	}

	return &readings, nil
}

func failOnError(err error, msg string) {
	if err != nil {
		slog.Error(fmt.Sprintf("Error: %s:  %v", msg, err))
		panic("error")
	}
}

type SyncMessage struct {
	NumMonths int `json:"num_months"`
}

func GetMonthBoundaries(yearMonth string) (string, string, error) {
	startLayout := "2006-01"
	startTime, err := time.Parse(startLayout, yearMonth)
	if err != nil {
		return "", "", err
	}
	endTime := startTime.AddDate(0, 1, 0).Add(-time.Second)
	outputLayout := "2006-01-02T15:04:05"
	return startTime.Format(outputLayout), endTime.Format(outputLayout), nil
}

func SyncMeterForMonth(resourceId, yearMonth, glowmarktToken, resourceType string, writeApi api.WriteApiBlocking) error {
	start, end, err := GetMonthBoundaries(yearMonth)
	if err != nil {
		return err
	}
	readings, err := GetDailyReadings(glowmarktToken, resourceId, start, end)
	if err != nil {
		slog.Error("Failed to get readings from glowmarkt API", "error", err)
		return err
	}
	slog.Info("Got readings from meter", "resourceId", resourceId, "numReadings", len(readings.Data), "start", start, "end", end)
	var points []*write.Point
	for _, reading := range readings.Data {
		unixTimestamp := int64(reading[0])
		timestamp := time.Unix(unixTimestamp, 0).UTC()
		p := influxdb2.NewPoint(
			"meter_reading",
			map[string]string{"meter_id": resourceId, "type": resourceType},
			map[string]interface{}{"cost_pence": reading[1]},
			timestamp,
		)
		points = append(points, p)
	}

	err = writeApi.WritePoint(context.Background(), points...)
	if err != nil {
		slog.Error("Failed to write points to influx", "error", err)
		return fmt.Errorf("failed to write points to InfluxDB: %w", err)
	}
	return err
}

func SyncMetersForMonth(config Config, token, yearMonth string, writeApi api.WriteApiBlocking) error {
	err := SyncMeterForMonth(config.electricMeterResourceId, yearMonth, token, "electricity", writeApi)
	if err != nil {
		slog.Error("Failed to sync with electric meter", "resource", config.electricMeterResourceId, "error", err)
		return err
	}
	err = SyncMeterForMonth(config.gasMeterResourceId, yearMonth, token, "gas", writeApi)
	if err != nil {
		slog.Error("Failed to sync with gas meter", "resource", config.electricMeterResourceId, "error", err)
		return err
	}
	return nil
}

func SyncMetersForMonths(config Config, numPriorMonths int, writeApi api.WriteApiBlocking) error {
	// SyncMetersForMonths syncs the meters for a number of months prior to and including the current month
	ratePeriodSeconds := 5
	token, err := Authenticate(config.glowmarktUsername, config.glowmarktPassword)
	if err != nil {
		slog.Error("Failed to authenticate with Glowmarkt", "error", err)
		return err
	}
	slog.Info("Authenticated with Glowmarkt")

	now := time.Now()

	layout := "2006-01"
	for i := range numPriorMonths {
		if i > 0 {
			// Probably not really an issue but just to be safe
			slog.Info("Sleeping for rate limit purposes", "seconds", ratePeriodSeconds)
			time.Sleep(time.Duration(ratePeriodSeconds) * time.Second)
		}
		monthStr := now.AddDate(0, -i, 0).Format(layout)
		err = SyncMetersForMonth(config, token, monthStr, writeApi)
		if err != nil {
			slog.Error("Failed to process month", "error", err, "month", monthStr)
			return err
		}
	}
	return nil
}

type QueryInputs struct {
	Bucket      string
	StartTime   string
	UtilityType string
}

func ReadMonthlyReadigsFromInflux(queryApi api.QueryAPI, meterType, startMonth, influxBucket string) (map[string]int64, error) {
	slog.Info("Reading records from influx", "type", meterType, "startMonth", startMonth, "influxBucket", influxBucket)
	fluxTemplate := `
			from(bucket: "{{.Bucket}}")
			  |> range(start: time(v: "{{.StartTime}}"), stop: now())
			  |> filter(fn: (r) => r["_measurement"] == "meter_reading")
			  |> filter(fn: (r) => r["_field"] == "cost_pence")
			  |> filter(fn: (r) => r["type"] == "{{.UtilityType}}")
			  |> aggregateWindow(every: 1mo, fn: sum, createEmpty: false)
			  |> yield(name: "monthly_sum")
		`

	queryParams := QueryInputs{
		Bucket:      influxBucket,
		StartTime:   fmt.Sprintf("%s%s", startMonth, "-01T00:00:00Z"),
		UtilityType: meterType,
	}

	tmpl, err := template.New("flux").Parse(fluxTemplate)
	if err != nil {
		slog.Error("Failed to parse flux template", "error", err)
		return nil, err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, queryParams); err != nil {
		slog.Error("Failed to create influx query", "error", err)
		return nil, err
	}
	result, err := queryApi.Query(context.Background(), buf.String())
	if err != nil {
		slog.Error("Failed to query influx", "error", err)
		return nil, err
	}
	monthlyCosts := make(map[string]int64)

	for result.Next() {
		timestamp := result.Record().Time()
		value := result.Record().Value()
		monthName := timestamp.Format("2006-01")
		if val, ok := value.(float64); ok {
			monthlyCosts[monthName] = int64(val)
		} else {
			slog.Error("Unexpected type for influx response", "value", value)
			return nil, fmt.Errorf("Failed to parse influx response")
		}
	}

	if result.Err() != nil {
		slog.Error("query error", "error", result.Err())
		return nil, fmt.Errorf("query error")
	}
	return monthlyCosts, nil
}

type ReadingDetails struct {
	AmountPence int `json:"amount_pence"`
}

type ApiResponse struct {
	Readings map[string]ReadingDetails `json:"readings"`
}

type Server struct {
	QueryApi api.QueryAPI
	Config   Config
}

func (s *Server) readingsHandler(w http.ResponseWriter, r *http.Request) {
	// Expected path format: /api/{electricity|gas}/readings
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) != 3 || pathParts[0] != "api" || pathParts[2] != "readings" {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}
	utilityType := pathParts[1]
	if utilityType != "electricity" && utilityType != "gas" {
		http.Error(w, "Invalid utility type. Must be 'electricity' or 'gas'", http.StatusBadRequest)
		return
	}
	sinceParam := r.URL.Query().Get("since")
	if sinceParam == "" {
		http.Error(w, "Missing required query parameter 'since'", http.StatusBadRequest)
		return
	}
	_, err := time.Parse("2006-01", sinceParam)
	if err != nil {
		http.Error(w, "Invalid 'since' format. Expected YYYY-MM", http.StatusBadRequest)
		return
	}

	readings, err := ReadMonthlyReadigsFromInflux(s.QueryApi, utilityType, sinceParam, s.Config.influxBucket)
	if err != nil {
		slog.Error("Failed to read from influx", "utilityType", utilityType, "sinceParam", sinceParam, "error", err)
		http.Error(w, "Failed to read data", http.StatusInternalServerError)
		return
	}

	response := ApiResponse{
		Readings: make(map[string]ReadingDetails),
	}

	for month, amountPence := range readings {
		response.Readings[month] = ReadingDetails{
			AmountPence: int(amountPence),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("Failed to encode JSON response", "error", err)
	}
}

func main() {
	config := loadConfig()

	influxClient := influxdb2.NewClient(config.influxEndpoint, config.influxToken)
	writeApi := influxClient.WriteAPIBlocking(config.influxOrg, config.influxBucket)
	queryApi := influxClient.QueryAPI(config.influxOrg)

	mqConn, err := amqp.Dial(config.rabbitmqConnectionString)
	failOnError(err, "Failed to connect to rmq")
	ch, err := mqConn.Channel()
	failOnError(err, "Failed to open rmq channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"smart-meter-sync", // name
		true,               // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		amqp.Table{},       // arguments
	)
	failOnError(err, "Failed to declare queue")

	msgs, err := ch.Consume(
		q.Name, // queue name
		"",     // consumer tag
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	// 5. Start a goroutine to listen for messages continuously
	go func() {
		for d := range msgs {
			// Parse the JSON payload into our struct
			var syncMsg SyncMessage
			err := json.Unmarshal(d.Body, &syncMsg)
			if err != nil {
				slog.Error("Error decoding JSON", "error", err)
				continue
			}

			slog.Info("Triggering sync for prior months", "numberMonths", syncMsg.NumMonths)
			err = SyncMetersForMonths(config, syncMsg.NumMonths, writeApi)
			if err != nil {
				slog.Error("Failed to sync meters", "error", err)
			}
		}
	}()

	slog.Info("Waiting for messages on queue...")
	srv := &Server{
		QueryApi: queryApi,
		Config:   config,
	}

	http.HandleFunc("/api/", srv.readingsHandler)

	slog.Info("Starting HTTP server on :8080...")
	// This call blocks main so the application stays alive,
	// while the background goroutine keeps listening to RabbitMQ/SQS.
	if err := http.ListenAndServe(":8080", nil); err != nil {
		slog.Error("HTTP server failed to start", "error", err)
		os.Exit(1)
	}
}

func getEnv(name string) string {
	s, ok := os.LookupEnv(name)
	if !ok {
		panic("No env variable " + name)
	}
	return s
}

func optionalEnv(name string) string {
	s, _ := os.LookupEnv(name)
	return s
}

func loadConfig() Config {
	return Config{
		glowmarktUsername:        getEnv("GLOWMARKT_USERNAME"),
		glowmarktPassword:        getEnv("GLOWMARKT_PASSWORD"),
		gasMeterResourceId:       getEnv("GAS_METER_RESOURCE_ID"),
		electricMeterResourceId:  getEnv("ELECTRIC_METER_RESOURCE_ID"),
		influxEndpoint:           optionalEnv("INFLUX_ENDPOINT"),
		influxToken:              optionalEnv("INFLUX_TOKEN"),
		influxOrg:                optionalEnv("INFLUX_ORG"),
		influxBucket:             optionalEnv("INFLUX_BUCKET"),
		rabbitmqConnectionString: optionalEnv("RABBITMQ_CONNECTION_STRING"),
	}
}
